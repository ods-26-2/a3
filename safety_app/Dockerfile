# Usa uma imagem oficial leve do Python
FROM python:3.11-slim

# Evita geração de arquivos .pyc e força logs em tempo real no terminal
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instala dependências de sistema úteis para vídeo e compilação de pacotes (ex: Kafka/OpenCV)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ffmpeg \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho no container
WORKDIR /app

# Copia e instala as dependências Python primeiro (aproveita o cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia o restante do código do projeto (incluindo pages/, services/, components/, db/, data/)
COPY . .

# Expõe a porta padrão do Streamlit (ajuste se usar FastAPI/Flask na 8000 ou 5000)
EXPOSE 8501

# Comando para iniciar a aplicação
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]