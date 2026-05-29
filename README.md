# Trabalho Prático 2 — Sistema P2P de Transferência de Arquivos

## O que é o trabalho

Este é o Trabalho Prático 2 da disciplina de **Sistemas Distribuídos** do CEFET-MG. O objetivo é implementar um sistema de transferência de arquivos ponto-a-ponto (P2P) em Python, inspirado no modelo BitTorrent, onde um arquivo é dividido em blocos e distribuído entre múltiplos peers na rede.

---

## Objetivo

Simular uma rede P2P onde:

- Um **seeder** possui o arquivo completo e o serve em blocos para os demais peers.
- Um ou mais **leechers** baixam os blocos de seus vizinhos (seeders ou outros leechers que já possuam partes do arquivo), remontam o arquivo original e passam a servir os blocos que possuem.
- A integridade de cada bloco é verificada via **SHA-256** antes de ser aceito.

---

## O que foi implementado

| Componente | Descrição |
|---|---|
| `p2p/models.py` | Estruturas de dados: `Block` (bloco com índice e dados) e `FileMetadata` (metadados do arquivo: nome, tamanho, hashes dos blocos) |
| `p2p/protocol.py` | Protocolo binário TCP com header de 8 bytes. Mensagens: `HANDSHAKE`, `BLOCK_REQUEST`, `BLOCK_RESPONSE`, `BLOCK_NOT_FOUND`, `METADATA_REQUEST`, `METADATA_RESPONSE` |
| `p2p/transfer.py` | `FileFragmenter` (divide arquivo em blocos), `FileAssembler` (remonta e valida integridade), `ChecksumUtil` (SHA-256 de arquivos e bytes) |
| `p2p/block_registry.py` | `BlockRegistry` — controle thread-safe de quais blocos o peer já possui |
| `p2p/client.py` | `PeerClient` (conecta a um vizinho e baixa blocos ausentes) e `DownloadManager` (download paralelo a partir de múltiplos vizinhos com retry) |
| `p2p/config.py` | `PeerConfig` — configuração do peer lida de um arquivo JSON |
| `p2p/logging_config.py` | Configuração de logs por peer |
| `p2p/main.py` | Ponto de entrada; inicializa o peer como seeder ou leecher com base no JSON de configuração |
| `tests/unit/` | Testes unitários para fragmentação, montagem, protocolo, registro de blocos, cliente e servidor |
| `tests/e2e/helpers.py` | Utilitários para testes de integração (geração de arquivo, espera por porta TCP, espera por marcador `.done`) |

---

## Como rodar

### Pré-requisitos

- Python 3.8+
- Instalar dependências:

```bash
pip install -r requirements.txt
```

### Configuração

Cada peer é configurado por um arquivo JSON. Exemplos:

**seeder.json**
```json
{
  "host": "127.0.0.1",
  "port": 5000,
  "neighbors": [],
  "block_size": 1024,
  "role": "seeder",
  "file_path": "arquivo.bin",
  "output_dir": "output/seeder"
}
```

**leecher.json**
```json
{
  "host": "127.0.0.1",
  "port": 5001,
  "neighbors": [
    {"host": "127.0.0.1", "port": 5000}
  ],
  "block_size": 1024,
  "role": "leecher",
  "output_dir": "output/leecher"
}
```

### Iniciando os peers

Primeiro inicie o seeder, depois o(s) leecher(s):

```bash
# Terminal 1 — Seeder
python -m p2p.main seeder.json

# Terminal 2 — Leecher
python -m p2p.main leecher.json
```

O leecher baixa todos os blocos, verifica a integridade de cada um via SHA-256, remonta o arquivo no `output_dir` e cria um marcador `.done` ao concluir. Em seguida, passa a servir os blocos que possui para outros peers.

### Rodando os testes

```bash
pytest
```

Para ver logs detalhados:

```bash
pytest -v
```
