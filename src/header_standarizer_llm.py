"""
Sistema de estandarización de encabezados usando LLM
Genera nombres cortos y manejables para columnas con nombres complejos
"""
#----------------------------
# LIBRERÍAS
#----------------------------
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline

import os
from dotenv import load_dotenv

#----------------------------
# VARIABLES DE ENTORNO
#----------------------------
load_dotenv("./variables_local.env")
MAPPING_HEADERS_NAME = os.getenv("MAPPING_HEADERS_NAME")
MAPPING_HEADERS_FILE = os.getenv("MAPPING_HEADERS_FILE")


#----------------------------
# CONFIGURACIONES LOGGING
#----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("HeaderStandardizer")


#----------------------------
# INICIO CÓDIGO
#----------------------------

class HeaderStandardizer:
    """
    Estandariza encabezados de columnas usando un LLM y mantiene
    un mapeo persistente en JSON con hashing para búsqueda rápida.
    """

    def __init__(
        self,
        model_name: str = "google/flan-t5-base",
        mappings_file: str = MAPPING_HEADERS_FILE,
        hash_length: int = 12
    ):
        """
        Args:
            model_name: Modelo HuggingFace a utilizar
            mappings_file: Archivo JSON para persistir mapeos
            hash_length: Longitud del hash (caracteres)
        """
        self.mappings_file = Path(mappings_file)
        self.hash_length = hash_length
        self.mappings: Dict[str, Dict] = {}

        # Inicializar modelo
        logger.info(f"Inicializando modelo {model_name}...")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        self.model.to(self.device)
        logger.info(f"Modelo cargado. Device: {self.device}")

        # Cargar mapeos existentes
        self._load_mappings()

    def _compute_hash(self, text: str) -> str:
        """Genera hash SHA256 truncado del texto."""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:self.hash_length]

    def _load_mappings(self) -> None:
        """Carga mapeos desde archivo JSON si existe."""
        if self.mappings_file.exists():
            logger.info(f"Cargando mapeos desde {self.mappings_file}")
            with open(self.mappings_file, 'r', encoding='utf-8') as f:
                self.mappings = json.load(f)
            logger.info(f"Cargados {len(self.mappings)} mapeos existentes")
        else:
            logger.info("No se encontró archivo de mapeos, iniciando desde cero")
            self.mappings = {}

    def _save_mappings(self) -> None:
        """Guarda mapeos actuales a archivo JSON."""
        logger.info(f"Guardando mapeos en {self.mappings_file}")
        with open(self.mappings_file, 'w', encoding='utf-8') as f:
            json.dump(self.mappings, f, indent=2, ensure_ascii=False)
        logger.info(f"Guardados {len(self.mappings)} mapeos")

    def _generate_standard_name(self, original_header: str) -> str:
        """
        Genera nombre estandarizado usando LLM.

        Args:
            original_header: Encabezado original (potencialmente largo/complejo)

        Returns:
            Nombre estandarizado corto (máx 3-4 palabras en snake_case)
        """
        # Prompt más directo y simple
        prompt = f"""Simplifica este encabezado a 4-6 palabras en español, snake_case, sin tildes y uppercase:
                    Entrada: "Marca"
                    Salida: "MARCA"

                    Entrada: "Emisiones de CO2 combinado (g/km)"
                    Salida: CO2_COMBINADO_GKM

                    Entrada: "Artículo 4° Nonies Decreto Supremo"
                    Salida: ARTICULO_4_DS

                    Entrada: "Ciclo WLTC Híbrido Recarga Exterior CO2 CS"
                    Salida: WLTC_HIB_RECARGAEXT_CO2_CS

                    Entrada: "{original_header}"
                    Salida:"""

        logger.debug(f"Generando nombre para: {original_header[:50]}...")

        # Tokenizar
        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            max_length=512,
            truncation=True
        ).to(self.device)

        # Generar
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=20,
                num_beams=1,
                do_sample=False,
                temperature=1.0
            )

        # Decodificar
        result = self.tokenizer.decode(outputs[0], skip_special_tokens=True)

        # Limpiar resultado
        standard_name = result.strip().upper()
        # Remover cualquier texto residual del prompt
        standard_name = standard_name.split('\n')[0].split(':')[-1].strip()
        # Convertir a snake_case válido
        standard_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in standard_name)
        standard_name = '_'.join(filter(None, standard_name.split('_')))  # Eliminar _ duplicados

        # Si el resultado está vacío o muy corto, usar fallback basado en hash
        if len(standard_name) < 3:
            logger.warning(f"Resultado vacío del LLM, usando fallback")
            standard_name = f"col_{self._compute_hash(original_header)[:8]}"

        logger.info(f"'{original_header[:40]}...' -> '{standard_name}'")
        return standard_name

    def standardize_header(self, original_header: str) -> str:
        """
        Estandariza un encabezado. Usa caché si ya existe, sino genera nuevo.

        Args:
            original_header: Encabezado original

        Returns:
            Nombre estandarizado
        """
        header_hash = self._compute_hash(original_header)

        # Buscar en caché por hash
        for std_name, info in self.mappings.items():
            if header_hash in info['hashes']:
                logger.debug(f"Encontrado en caché: {std_name}")
                return std_name

        # No encontrado, generar nuevo
        logger.info(f"Encabezado nuevo detectado, generando nombre estándar...")
        standard_name = self._generate_standard_name(original_header)

        # Evitar colisiones de nombres
        base_name = standard_name
        counter = 1
        while standard_name in self.mappings:
            standard_name = f"{base_name}_{counter}"
            counter += 1

        # Almacenar en mappings
        self.mappings[standard_name] = {
            "original_names": [original_header],
            "hashes": [header_hash],
            "created_at": str(Path(__file__).stat().st_mtime)
        }

        self._save_mappings()
        return standard_name

    def batch_standardize(self, headers: List[str]) -> Dict[str, str]:
        """
        Estandariza múltiples encabezados de una vez.

        Args:
            headers: Lista de encabezados originales

        Returns:
            Diccionario {original: estandarizado}
        """
        logger.info(f"Procesando batch de {len(headers)} encabezados...")
        mapping = {}

        for header in headers:
            std_name = self.standardize_header(header)
            mapping[header] = std_name

        logger.info(f"Batch completado: {len(mapping)} encabezados estandarizados")
        return mapping

    def get_mapping_info(self, standard_name: str) -> Optional[Dict]:
        """Obtiene información completa de un nombre estandarizado."""
        return self.mappings.get(standard_name)

    def export_to_csv(self, output_file: str = "header_mappings.csv") -> None:
        """Exporta mapeos a CSV para inspección."""
        import pandas as pd

        rows = []
        for std_name, info in self.mappings.items():
            for orig, hash_val in zip(info['original_names'], info['hashes']):
                rows.append({
                    'standard_name': std_name,
                    'original_name': orig,
                    'hash': hash_val
                })

        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        logger.info(f"Mapeos exportados a {output_file}")


def main():
    """Ejemplo de uso con los encabezados proporcionados."""

    # Encabezados de ejemplo
    headers_ejemplo = [
        'Artículo 4° Nonies, D.S. 211/91 Ministerio de Transportes y Telecomunicaciones (Estándar Superior)',
        'CO2 combinado_(g/km)',
        'Categoría vehículo',
        'Ciclo WLTC\nVehículo Híbrido con Recarga Exterior\nEmisiones de CO2\n_CONDICIÓN COMBINADO CS (*) g/km',
        'Ciclo WLTC\nVehículo Híbrido con Recarga Exterior\nEmisiones de CO2\n_CONDICIÓN COMBINADO CD (**) g/km',
        'Ciclo WLTC\nVehículo Híbrido con Recarga Exterior Emisiones de CO2 PONDERADA g/km',
        'Ciclo WLTC Vehículo Híbrido con Recarga Exterior Consumo de combustible (km/l) (*):_Condición CS: Mantenimiento de carga_Urbano sin Autopista (km/l)'
    ]

    # Inicializar estandarizador
    standardizer = HeaderStandardizer(
        mappings_file=MAPPING_HEADERS_FILE
    )

    # Procesar batch
    mapping = standardizer.batch_standardize(headers_ejemplo)

    # Mostrar resultados
    print("\n" + "="*80)
    print("MAPEO DE ENCABEZADOS")
    print("="*80)
    for orig, std in mapping.items():
        print(f"\nOriginal: {orig[:60]}...")
        print(f"Estándar: {std}")

    # Exportar a CSV
    standardizer.export_to_csv(f"data/{MAPPING_HEADERS_NAME}.csv")

    print("\n" + "="*80)
    print(f"Proceso completado. Mapeos guardados en {standardizer.mappings_file}")
    print("="*80)


if __name__ == "__main__":
    main()
