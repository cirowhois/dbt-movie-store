# Play Movies
![play-movies-logo](https://github.com/cirowhois/dbt-ciro/blob/d453c8be77f93484341cb376df70c9d2472c7a26/models/movie_store/play%20movies.png)

A **Play Movies** é uma empresa especializada em locação de filmes para o mundo todo. Por esta razão, precisamos de dados que nos ajudem a entender o nosso próprio negócio, nosso público e oportunidades de aumentar nossa eficiência.

# Repositório de Dados do time de Analytics da Play Movies

Nosso repositório é integrado ao Dbt e possui todas as queries e testes que organizam os dados de nossos filmes, nossos clientes e de nossas lojas no formato de star schema. É aqui, portanto, que versionamos todo o nosso processo de transformação de dados.
Nossos dados raw ficam armazenados em um espaço no Google BigQuery constantemente alimentados pelos nossos sistemas que integram este serviço aos ERPs de cada loja. Depois de transformados via Dbt, eles são consolidados no nosso Data Warehouse que também fica em outro espaço no Google BigQuery.
Finalmente, estes dados são apresentados às partes interessadas no nosso Dashboard geral no Looker Studio em: 
https://lookerstudio.google.com/reporting/78b3c2de-1126-4b7a-9f3e-e746a9193845/page/8YXSD .