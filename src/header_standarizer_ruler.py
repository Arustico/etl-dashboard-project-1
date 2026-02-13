"""
Sistema de estandarización de encabezados usando REGLAS
Alternativa más confiable y rápida que LLM para casos predecibles
"""
#----------------------------
# LIBRERÍAS
#----------------------------
import json
import hashlib
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional
from unidecode import unidecode


import os
from dotenv import load_dotenv

#----------------------------
# VARIABLES DE ENTORNO
#----------------------------
load_dotenv("./variables_local.env")

FOLDER_PROCESSED = os.getenv("FOLDER_PROCESSED")
MAPPING_HEADERS_NAME = os.getenv("MAPPING_HEADERS_NAME")
MAPPING_HEADERS_FILE = os.getenv("MAPPING_HEADERS_FILE")
MAPPING_HEADERS_CSV = f"{FOLDER_PROCESSED}{MAPPING_HEADERS_NAME}.csv"

#----------------------------
# CONFIGURACIONES LOGGING
#----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
name_script = Path(__file__)
logger = logging.getLogger(f"Estandarizador de Encabezados con Reglas - {name_script}")


#----------------------------
# INICIO CODIGO
#----------------------------

class HeaderStandardizerRules:
    """
    Estandariza encabezados usando reglas y normalización de texto.
    Más rápido y confiable que LLM para patrones conocidos.
    """

    def __init__(
        self,
        mappings_file: str = MAPPING_HEADERS_FILE,
        hash_length: int = 12,
        maxlenHeader: int = 10
    ):
        self.mappings_file = Path(mappings_file)
        self.hash_length = hash_length
        self.mappings: Dict[str, Dict] = {}
        self.maxlenHeader = maxlenHeader

        # Términos prioritarios que SIEMPRE deben incluirse
        # Organizados por categorías para mayor claridad
        # NOTA: Usar forma abreviada si existe en abbreviations
        self.priority_terms = {
            # Condiciones de prueba
            'combinado', 'comb', 'mixto', 'urbano', 'urb', 'carretera', 'carr', 'autopista', 'autop','ciudad','rural'
            # Estados/modos
            'ponderado', 'pond', 'ponderada', 'maximo', 'maxima', 'minimo', 'minima','hight','low',
            # Tipos de vehículo
            'phev', 'hev', 'ev','h2',
            # Condiciones técnicas
            'cs', 'cd','epa','eu' #'wltc', 'nedc', ,
            # Características importantes
            'rendimiento','rend','emision','emis','potencia','pot' #'recarga', 'exterior', 'ext', 'interior',
            # Partes/componentes
            'motor', 'bateria','bat', 'tanque',
            # gases
            'nox','co','co2','hc','hcnm','nmog','hc+nox','mp','np','hcho','hcnm','n2o','nmog+nox',
        }

        # Unidades de medidas
        self.measure_units = {
            'km', 'kmh', 'kml', 'kmkwh',  # Distancia/velocidad
            'kw', 'kwh', 'cv', 'hp',       # Potencia/energía
            'kg', 'g', 'mg', 't',          # Masa
            'l', 'ml', 'gal','lts',        # Volumen
            'm', 'cm', 'mm',               # Longitud
            'rpm', 'nm', 'bar', 'psi',     # Mecánicas
            'gkm', 'gkwh', 'grkm',         # Emisiones
        }
        # Reglas de abreviación especiales del dominio
        self.special_abbreviations = {
            'hibrido con recarga exterior': 'phev',
            'hibrido sin recarga exterior': 'hev',
            'masa de perticula': 'mp',
            'numero de particula': 'np',
            'norma europea': 'emision eu',
            'norma usa epa 50 000 / 120 000 150 000 millas': 'emision epa',
            'mantenimiento de carga': 'manten carga',
            'sin autopista': '',
            'con autopista': 'autop',
            'autopista interurbana': 'auto interurb',
            'p b v': 'peso bruto vh',
            'grkm': 'gkm',
            }
        # Reglas de abreviación comunes en el dominio
        self.abbreviations = {
            'artículo': 'art',
            'distancia':'dist',
            'hidrogeno':'h2',
            'd':'ds',
            's':'ds',
            'maxima':'max',
            'minima':'min',
            'decreto': 'ds',
            'supremo': 'ds',
            'ministerio': 'mtt',
            'transportes': 'mtt',
            'telecomunicaciones': 'mtt',
            'vehículo': 'vh',
            'híbrido': 'hib',
            'eléctrico': 'ev',
            'recarga': 'recarga',
            'exterior': 'ext',
            'emisiones': 'emis',
            'consumo': 'consum',
            'combustible': 'combustible',
            'rendimiento': 'rend',
            'urbano': 'urb',
            'autopista': 'autop',
            'combinado': 'comb',
            'ponderada': 'pond',
            'categoría': 'categoria',
            'kilómetros': 'km',
            'gramos': 'g',
            'litros': 'l',
            'litro': 'l',
            'peso': 'peso',
            'bruto': 'bruto',
            'vehicular': 'vh',
            'vehiculos': 'vh',
            'sin':'sin',
            'kg':'kg',
            'duales':'',
            'gasolina':'gas',
            'europea': "eur",
            'homologación':'homl',
            'estándar':'estd',
            'superior':'sup',
            'potencia':'pot',
        }

        # estandarización de abreviaciones:
        self.abbreviations = {unidecode(kw):w for kw,w in self.abbreviations.items()}
        self.special_abbreviations = {unidecode(kw):w for kw,w in self.special_abbreviations.items()}

        # Patrones a eliminar
        self.remove_patterns = [
            #r'\([^)]*\)',   # Contenido entre paréntesis
            r'\(',r'\)',     # Paréntesis, nos quedamos con el contenido
            r'\*+',         # Asteriscos
            r'°',           # Grado
            r'\d{1,3}/\d{2,4}',  # Referencias tipo "211/91"
            r'[\n\r\t]+',   # Saltos de línea y tabs
            r'[_\-]{1,}',   # Múltiples guiones o underscores
            r'[\.\,]' #puntos y comas
        ]

        self._load_mappings()
        logger.info(f"{name_script} inicializado")

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
            logger.info("No se encontró archivo de mapeos")
            self.mappings = {}

    def _save_mappings(self) -> None:
        """Guarda mapeos actuales a archivo JSON."""
        logger.info(f"Guardando mapeos en {self.mappings_file}")
        with open(self.mappings_file, 'w', encoding='utf-8') as f:
            json.dump(self.mappings, f, indent=2, ensure_ascii=False)
        logger.info(f"Guardados {len(self.mappings)} mapeos")

    # Acá ver cómo normalizar unidades de medidas. g-> gr,gramos,g, grams, etc.
    def _normalize_measure_unit(self, measure_unit):
        return None

    def _normalize_text(self, text: str) -> str:
        """
        Normaliza texto: minúsculas, sin tildes, sin espacios extra.
        """
        # Convertir a minúsculas
        text = text.lower()
        # Remover patrones no deseados
        for pattern in self.remove_patterns:
            text = re.sub(pattern, ' ', text)
        # Remover tildes y caracteres especiales
        text = unidecode(text)
        # Limpiar espacios
        text = ' '.join(text.split())
        return text

    def _apply_abbreviations(self, text: str) -> str:
        """
        Aplica abreviaciones conocidas del dominio. Y significados especiales
        """
        for word, abbrev in self.special_abbreviations.items():
            text = text.replace(word,abbrev)

        words = text.split()
        abbreviated = []

        for word in words:
            # Buscar abreviación
            abbr = self.abbreviations.get(word, word)
            abbreviated.append(abbr)

        return ' '.join(abbreviated)


    def _extract_measure_unit(self, text: str) -> Optional[str]:
        """
        Extrae unidad de medida del texto si existe.
        Busca patrones como: (g/km), km/h, kWh, etc.

        Returns:
            Unidad normalizada sin separadores o None si no hay
        """
        # Patrón para unidades con formato: letra+número?/letra+número?
        # Captura: (g/km), km/h, kWh, km/kWh, etc.
        pattern = r"\(?\b([a-zA-Z]+\d*(?:/[a-zA-Z]+\d*)+)\b\)?"

        match = re.search(pattern, text)
        if match:
            unit = match.group(1).lower()
            # Normalizar: remover separadores
            unit_normalized = unit.replace('/', '')
            logger.debug(f"Unidad encontrada por patrón: {unit} -> {unit_normalized}")
            return unit_normalized

        # Buscar en lista de unidades conocidas
        text_lower = text.lower()
        for unit in self.measure_units:
            # Buscar la unidad como palabra completa
            if re.search(rf'\b{unit}\b', text_lower):
                logger.debug(f"Unidad encontrada en lista: {unit}")
                return unit
        return None


    def _extract_key_terms(self, text: str, max_terms: int = 8) -> List[str]:
        """
        Extrae términos clave con sistema de priorización.

        Prioridad:
        1. Términos prioritarios (siempre incluidos)
        2. Términos no-stopwords por orden de aparición

        Args:
            text: Texto normalizado y abreviado
            max_terms: Máximo de términos a extraer

        Returns:
            Lista ordenada de términos clave (max max_terms elementos)
        """
        # Palabras a ignorar (stopwords en español)
        stopwords = {
            'de', 'del', 'la', 'el', 'los', 'las', 'un', 'una', 'y', 'o',
            'en', 'con', 'sin', 'por', 'para', 'a', 'al', 'se', 'su',
            'que', 'es', 'son', 'esta', 'este', 'mediante', 'segun','nonies',
            'ciclo','condicion','puro','entre'
        }

        words = text.split()

        # Paso 1: Identificar términos prioritarios presentes
        priority_found = []
        for word in words:
            if word in self.priority_terms and word not in priority_found:
                priority_found.append(word)

        # Paso 2: Extraer términos regulares (no stopwords, no prioritarios)
        regular_terms = []
        for word in words:
            if (word not in stopwords and
                word not in self.priority_terms and
                len(word) > 1 and
                word not in regular_terms):
                regular_terms.append(word)

        # Paso 3: Combinar priorizando términos importantes
        # Los prioritarios van primero, luego completamos con regulares
        key_terms = priority_found + regular_terms

        # Paso 4: Limitar a max_terms
        result = key_terms[:max_terms]

        if priority_found:
            logger.debug(f"Términos prioritarios encontrados: {priority_found}")

        return result

    def _to_snake_case(self, words: List[str]) -> str:
        """
        Convierte lista de palabras a snake_case válido.
        """
        # Unir con underscore (evitamos duplicados)
        #snake = '_'.join(words)
        snake = '_'.join(f"{wd}" for wd in dict.fromkeys(words)) # dict.fromkeys(words) es un set pero con orden.
        # Asegurar solo caracteres válidos
        snake = re.sub(r'[^a-z0-9_]', '_', snake)
        # Eliminar underscores múltiples
        snake = re.sub(r'_+', '_', snake)
        # Eliminar underscores al inicio/final
        snake = snake.strip('_')

        return snake

    def _generate_standard_name(self, original_header: str) -> str:
        """
        Genera nombre estandarizado usando reglas.
        """
        logger.debug(f"Procesando: {original_header[:50]}...")

        # Paso 1: Normalizar
        normalized = self._normalize_text(original_header)

        # Paso 2: Extraer unidad ANTES de procesar (para no perderla)
        measure_unit = self._extract_measure_unit(original_header)
        if measure_unit:
            pattern = r"\(?\b([a-zA-Z]+\d*(?:/[a-zA-Z]+\d*)+)\b\)?"
            normalized = re.sub(pattern,'',normalized)

        # Paso 3: Aplicar abreviaciones
        abbreviated = self._apply_abbreviations(normalized)

        # Paso 4: Extraer términos clave (máx 3 si hay unidad, 4 si no)
        max_terms = min([len(abbreviated.split()),self.maxlenHeader]) if measure_unit else self.maxlenHeader
        key_terms = self._extract_key_terms(abbreviated, max_terms=max_terms)

        # Paso 5: Agregar unidad al final si existe
        if measure_unit:
            key_terms.append(measure_unit)

        # Paso 6: Convertir a snake_case
        standard_name = self._to_snake_case(key_terms)

        # Paso 7: Validar longitud mínima
        if len(standard_name) < 3:
            logger.warning(f"Nombre muy corto, usando hash")
            standard_name = f"col_{self._compute_hash(original_header)[:8]}"

        logger.info(f"'{original_header[:40]}...' -> '{standard_name}'")
        return standard_name.upper() #nombre estandarizado en uppercase

    def standardize_header(self, original_header: str) -> str:
        """
        Estandariza un encabezado. Usa caché si existe.
        """
        header_hash = self._compute_hash(original_header)

        # Buscar en caché
        for std_name, info in self.mappings.items():
            if header_hash in info['hashes']:
                logger.debug(f"Encontrado en caché: {std_name}")
                return std_name

        # Generar nuevo
        logger.info(f"Generando nuevo nombre estándar...")
        standard_name = self._generate_standard_name(original_header)

        # Evitar colisiones
        base_name = standard_name
        counter = 1
        while standard_name in self.mappings:
            standard_name = f"{base_name}_{counter}"
            counter += 1

        # Almacenar
        self.mappings[standard_name] = {
            "original_names": [original_header],
            "hashes": [header_hash]
        }

        self._save_mappings()
        return standard_name

    def batch_standardize(self, headers: List[str]) -> Dict[str, str]:
        """
        Estandariza múltiples encabezados.
        """
        logger.info(f"Procesando batch de {len(headers)} encabezados...")
        mapping = {}

        for header in headers:
            std_name = self.standardize_header(header)
            mapping[header] = std_name

        logger.info(f"Batch completado")
        return mapping

    def export_to_csv(self, output_file: str = "header_mappings.csv") -> None:
        """Exporta mapeos a CSV."""
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
    """Demo con encabezados de ejemplo."""

    headers_ejemplo = [
        'Artículo 4° Nonies, D.S. 211/91 Ministerio de Transportes y Telecomunicaciones (Estándar Superior)',
        'CO2 combinado (g/km)',
        'Categoría vehículo',
        'Ciclo WLTC\nVehículo Híbrido con Recarga Exterior\nEmisiones de CO2\n CONDICIÓN COMBINADO CS (*) g/km',
        'Ciclo WLTC\nVehículo Híbrido con Recarga Exterior\nEmisiones de CO2\n CONDICIÓN COMBINADO CD (**) g/km',
        'Ciclo WLTC\nVehículo Híbrido con Recarga Exterior Emisiones de CO2 PONDERADA g/km',
        'Ciclo WLTC Vehículo Híbrido con Recarga Exterior Consumo de combustible (km/l) (*): Condición CS: Mantenimiento de carga Urbano sin Autopista (km/l)'
    ]

    print("\n" + "="*80)
    print("DEMO: ESTANDARIZACIÓN CON REGLAS")
    print("="*80)

    standardizer = HeaderStandardizerRules()
    mapping = standardizer.batch_standardize(headers_ejemplo)

    print("\nRESULTADOS:")
    print("-"*80)
    for orig, std in mapping.items():
        print(f"\nOriginal: {orig}")
        print(f"Estándar: {std}")

    standardizer.export_to_csv(MAPPING_HEADERS_CSV)

    print("\n" + "="*80)
    print("Proceso completado")
    print("="*80)


if __name__ == "__main__":
    main()
