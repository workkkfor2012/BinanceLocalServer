graph TD
    %% =============================================
    %% Phase 1: Define CSS-like classes for each layer (swimlane)
    %% =============================================
    classDef web_class fill:#D6EAF8,stroke:#3498DB,stroke-width:2px;
    classDef core_class fill:#D5F5E3,stroke:#2ECC71,stroke-width:2px;
    classDef infra_class fill:#FCF3CF,stroke:#F1C40F,stroke-width:2px;

    %% =============================================
    %% Phase 2: Define ALL nodes first
    %% =============================================
    A["HTTP GET Request"];
    B["handlers::binary_kline_handler"];
    C{"data: (symbol, interval)"};
    D["cache_manager::get_klines"];
    E{"decision: check cache"};
    F["calc start_time"];
    G["prepare full_fetch"];
    H["domain::DownloadTask"];
    I["api_client::download_..."];
    J["cloud: call external API (side effect)"];
    K{"returns data: Vec<Kline>"};
    L["cache_manager receives Vec<Kline>"];
    M["1. update mem_cache"];
    N["2. async call: db_manager::save_klines"];
    O["db: write to database (side effect)"];
    P["handler receives Vec<Kline>"];
    Q["transformer::klines_to_binary_blob"];
    R{"data: Vec<u8>"};
    S["HTTP 200 OK (binary response)"];

    %% =============================================
    %% Phase 3: Assign each node to a class (color)
    %% =============================================
    class A,B,C,P,Q,R,S web_class;
    class D,E,F,G,H,L,M,N core_class;
    class I,J,K,O infra_class;

    %% =============================================
    %% Phase 4: Define all connections
    %% =============================================
    A --> B;
    B --> C;
    C --> |call| D;
    D --> E;
    E -- hit --> F;
    E -- miss --> G;
    F --> H;
    G --> H;
    H --> |call| I;
    I --> J;
    J --> K;
    K --> |return| L;
    L --> M;
    M --> |return| P;
    P --> Q;
    Q --> R;
    R --> S;
    L -.-> |task::spawn| N;
    N --> O;