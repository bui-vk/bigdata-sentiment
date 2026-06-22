FROM python:3.10-slim
 
WORKDIR /app
 
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
 
# CPU-only PyTorch — much smaller than the default CUDA build (~800 MB vs ~3 GB)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY . .
 