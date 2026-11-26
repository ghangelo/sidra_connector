# SIDRA Connector

## Disclaimer — Uso de Inteligência Artificial

Este projeto utilizou ferramentas de inteligência artificial (IA) como assistentes durante o processo de desenvolvimento. A IA foi empregada para auxiliar na escrita, revisão e refatoração de código, bem como na geração de estruturas e documentação.

Todo o código gerado com auxílio de IA foi revisado, validado e adaptado pelo autor antes de ser incorporado ao projeto. A concepção, arquitetura, decisões de design e a lógica de negócio são de autoria e responsabilidade do desenvolvedor.

## Descrição

O **SIDRA Connector** é um plugin para o QGIS projetado para facilitar a integração de dados estatísticos do Sistema IBGE de Recuperação Automática (SIDRA) com as malhas territoriais digitais do IBGE. Ele permite que usuários, como analistas de dados, pesquisadores e planejadores urbanos, busquem, baixem e vinculem dados de tabelas do SIDRA diretamente a camadas vetoriais no QGIS.

Este projeto visa otimizar o fluxo de trabalho de análise geoespacial, eliminando a necessidade de baixar e processar dados manualmente.

## Compatibilidade

| QGIS | Qt | Status |
|---|---|---|
| 3.x | Qt5 / PyQt5 | ✅ |
| 4.x | Qt6 / PyQt6 | ✅ |

**Dependências:** Apenas a biblioteca padrão do Python e `requests` (já incluída no QGIS).

## Funcionalidades

*   **Assistente de Busca:** Pesquise tabelas do IBGE por nome, sigla (ex.: PIB, VAB, PAM, PPM, IPCA) ou ID com busca dinâmica em tempo real e construa a consulta de forma guiada.
*   **Download de Malhas Vetoriais:** Baixe malhas territoriais (municípios, estados, regiões imediatas/intermediárias, etc.) do IBGE diretamente como camadas no QGIS.
*   **Recorte Automático e União de Dados:** Detecta automaticamente as UFs da camada alvo para otimizar a consulta no SIDRA e vincula as estatísticas em uma nova camada vetorial com aliases legíveis.

## Como Usar

1.  **Abra o Plugin:** Clique no ícone do SIDRA Connector para abrir a janela principal.
2.  **Download da Malha (opcional):** Baixe uma malha vetorial correspondente usando a seção "1. Baixar Malha do IBGE" ou utilize uma camada já carregada no projeto.
3.  **Buscar Tabela:** Clique no botão **"Buscar Tabela..."** para abrir o assistente.
4.  **Pesquisar:** Digite o nome, palavras-chave, sigla ou o ID numérico da tabela (ex.: "pib municipios", "população", "5938") e selecione a tabela nos resultados.
5.  **Configurar:** Siga os diálogos interativos para selecionar:
    - Períodos (anos/meses)
    - Nível geográfico (Brasil, Estados, Municípios, etc.)
    - Variáveis da tabela
    - Categorias das classificações
6.  **Selecionar Camada e Campo:** Escolha a camada vetorial e o campo de código IBGE na seção "3. Unir Dados à Camada".
7.  **Executar União:** Clique em **"Unir à Camada"** para gerar a nova camada em memória com os dados unidos.

## Estrutura do Projeto

```
sidra_connector/
├── __init__.py              # Ponto de entrada do plugin
├── plugin.py                # Classe principal registrada no QGIS
├── metadata.txt             # Metadados do plugin
├── exceptions.py            # Exceções tipadas do plugin
├── core/                    # Lógica de negócio
│   ├── api_helpers.py       # Busca de metadados e montagem de URL
│   ├── data_joiner.py       # União de dados SIDRA com camadas vetoriais
│   ├── mesh_downloader.py   # Download e extração de malhas do IBGE
│   └── sidra_api_client.py  # Cliente da API SIDRA (Python puro)
├── gis/                     # Integração com QGIS
│   ├── layer_manager.py     # Gerenciamento de camadas
│   └── task_manager.py      # Tarefas assíncronas (QgsTask)
├── infrastructure/          # Rede QGIS e checkpoints locais
│   ├── checkpoint_store.py  # Persistência atômica de lotes em JSON
│   └── http_client.py       # Cliente HTTP via QgsBlockingNetworkRequest
├── services/                # Serviços de metadados e lotes adaptativos
│   ├── sidra_metadata_service.py
│   └── sidra_values_service.py
├── ui/                      # Interface do usuário
│   ├── custom_widgets.py        # Widgets personalizados
│   ├── main_dialog.py           # Lógica do diálogo principal
│   ├── main_dialog_base.ui      # Layout do diálogo (Qt Designer)
│   └── query_builder_dialog.py  # Assistente de busca de tabelas
├── utils/                   # Utilitários
│   └── constants.py         # Constantes (UFs, URLs, níveis, timeouts)
└── dev/                     # Ferramentas de desenvolvimento e empacotamento
    ├── criar_db.py          # Script para gerar agregados_ibge.db
    └── empacotar.py         # Script para gerar sidra_connector.zip
```

## Contribuições

Contribuições são bem-vindas! Se você encontrar um bug ou tiver uma sugestão, por favor, abra uma [issue](https://github.com/ghangelo/sidra_connector/issues).

## Autor

*   **Gabriel Henrique Angelo** - [ghangelo](https://github.com/ghangelo)

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo `LICENSE` para mais detalhes.
