# SIDRA Connector

## Descrição

O **SIDRA Connector** é um plugin para o QGIS projetado para facilitar a integração de dados estatísticos do Sistema IBGE de Recuperação Automática (SIDRA) com as malhas territoriais digitais do IBGE. Ele permite que usuários, como analistas de dados, pesquisadores e planejadores urbanos, busquem, baixem e vinculem dados de tabelas do SIDRA diretamente a camadas vetoriais no QGIS.

Este projeto visa otimizar o fluxo de trabalho de análise geoespacial, eliminando a necessidade de baixar e processar dados manualmente.

## Compatibilidade

| QGIS | Qt | Status |
|---|---|---|
| 3.x (até 3.38) | Qt5 / PyQt5 | ✅ Compatível |
| 3.40+ | Qt6 / PyQt6 | ✅ Compatível |
| 4.x (futuro) | Qt6 / PyQt6 | ✅ Pronto |

O plugin utiliza a camada de compatibilidade `qgis.PyQt`, portanto funciona em ambas as versões do Qt sem necessidade de alterações.

**Dependências:** Apenas a biblioteca padrão do Python e `requests` (já incluída no QGIS). Não requer `pandas` nem nenhum pacote externo.

## Funcionalidades

*   **Assistente de Busca:** Pesquise tabelas do IBGE por nome ou ID com busca dinâmica em tempo real e construa URLs da API de forma interativa e guiada.
*   **Download de Malhas Vetoriais:** Baixe malhas territoriais (municípios, estados, etc.) do IBGE como camadas temporárias no QGIS — sem arquivos residuais em disco.
*   **União de Dados:** Vincule os dados estatísticos baixados do SIDRA com as camadas vetoriais correspondentes de forma automática.

## Como Usar

1.  **Abra o Plugin:** Clique no ícone do SIDRA Connector para abrir a janela principal.

### Opção 1: Usando o Assistente de Busca (Recomendado)
2.  **Buscar Tabela:** Clique no botão "Buscar Tabela..." para abrir o assistente.
3.  **Pesquisar:** Digite o nome, palavras-chave ou o ID numérico da tabela e veja os resultados aparecerem em tempo real (ex: "população", "PIB", "5938").
4.  **Selecionar:** Clique na tabela desejada da lista de resultados.
5.  **Configurar:** Siga os diálogos interativos para selecionar:
    - Períodos (anos/meses)
    - Nível geográfico (Estados, Municípios, etc.)
    - Variáveis da tabela
    - Categorias das classificações
6.  **URL Automática:** A URL da API será gerada e inserida automaticamente.

### Opção 2: URL Manual
2.  **URL da API:** Cole diretamente uma URL da API do SIDRA no campo correspondente.

### Finalização
7.  **Download da Malha:** Opcionalmente, baixe uma malha vetorial correspondente usando a seção "Download de Malha".
8.  **Selecionar Camada:** Escolha uma camada vetorial já carregada no seu projeto.
9.  **Executar União:** Clique em "Buscar Dados e Unir à Camada Alvo" para processar e criar a nova camada com os dados unidos.

## Estrutura do Projeto

```
sidra_connector/
├── __init__.py          # Ponto de entrada do plugin
├── plugin.py            # Classe principal registrada no QGIS
├── metadata.txt         # Metadados do plugin
├── core/                # Lógica de negócio
│   ├── api_helpers.py       # Busca de metadados e montagem de URL
│   ├── data_joiner.py       # União de dados SIDRA com camadas vetoriais
│   ├── mesh_downloader.py   # Download e extração de malhas do IBGE
│   └── sidra_api_client.py  # Cliente da API SIDRA (Python puro)
├── gis/                 # Integração com QGIS
│   ├── layer_manager.py     # Gerenciamento de camadas
│   └── task_manager.py      # Tarefas assíncronas (QgsTask)
├── ui/                  # Interface do usuário
│   ├── custom_widgets.py        # Widgets personalizados
│   ├── main_dialog.py          # Lógica do diálogo principal
│   ├── main_dialog_base.ui     # Layout do diálogo (Qt Designer)
│   └── query_builder_dialog.py # Assistente de busca de tabelas
├── utils/               # Utilitários
│   └── constants.py         # Constantes (UFs, URLs, timeouts)
└── dev/                 # Ferramentas de desenvolvimento
    └── criar_db.py          # Script para gerar agregados_ibge.db
```

## Contribuições

Contribuições são bem-vindas! Se você encontrar um bug ou tiver uma sugestão, por favor, abra uma [issue](https://github.com/GaboV3/sidra_connector/issues).

## Autor

*   **Gabriel Henrique Angelo** - [GaboV3](https://github.com/GaboV3)

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo `LICENSE` para mais detalhes.
