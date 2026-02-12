from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
import torch

model_name = "google/flan-t5-base"
device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

generator = pipeline(
    task="text-generation",   # ← en v5 esto funciona con seq2seq
    model=model,
    tokenizer=tokenizer,
    device=device
)

prompt = "Suma 1 y 1 en binario"
response = generator(
    prompt,
    max_new_tokens=32,
    do_sample=False
)[0]["generated_text"]
