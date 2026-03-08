# XmlProcessor — Leitor de NF-e Refatorado

Aplicação para leitura de arquivos XML de Notas Fiscais Eletrônicas (NF-e),
extraindo impostos (ICMS, PIS, COFINS, IBS/CBS) e exportando para CSV/Excel.

## Estrutura de pastas

```
XmlProcessor/
│
├── main.py                  ← ponto de entrada (execute este)
│
├── config/
│   └── settings.py          ← caminhos, IDs de sessão, cabeçalho CSV
│
├── extract/
│   └── xml_reader.py        ← EXTRACT: lê e parseia o XML NF-e
│
├── transform/
│   └── validator.py         ← TRANSFORM: normaliza e deduplica produtos
│
├── load/
│   └── storage.py           ← LOAD: salva CSV/Excel, sincroniza, limpa temp
│
└── ui/
    └── main_window.py       ← interface gráfica (CustomTkinter)
```

## Como rodar

```bash
pip install pandas openpyxl customtkinter
python main.py
```

## Fluxo ETL

```
XML NF-e
   │
   ▼  extract/xml_reader.py
Lista de produtos (dicts)
   │
   ▼  transform/validator.py
Novos × Duplicados (normalizado)
   │
   ▼  load/storage.py
CSV temporário → Excel local → (sincroniza) → CSV/Excel principal
```

## Responsabilidades por módulo

| Módulo | Faz | Não faz |
|---|---|---|
| `config/settings.py` | Define constantes e caminhos | Lógica nenhuma |
| `extract/xml_reader.py` | Lê e parseia XML | Escreve em disco |
| `transform/validator.py` | Normaliza e deduplica | Lê/escreve disco |
| `load/storage.py` | Persiste dados, lock, sync | Lógica de negócio |
| `ui/main_window.py` | Interface e orquestração | Lógica de dados direta |
