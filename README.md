# Проект 7-го спринта

Исходные данные берутся по следующему адресу: /user/master/data/geo/events/date=YYYY-MM-DD
Последующая их обработка происходит в: /user/s19419897/
Справочник координат городов находится по адресу (вторая версия с часовыми поясами): /user/s19419897/data/geo_2.csv
Витрины:
- в разрезе пользователей (datamart_users): /user/s19419897/analytics/datamart_users
- в разрезе зон (datamart_zones): /user/s19419897/analytics/datamart_zones
- для рекомендации друзей (datamart_friends_recommendations): /user/s19419897/analytics/datamart_friends_recommendations

Все данные витрин хранятся в parquet

- datamart_users обновляется ежемесячно в 01:00
- datamart_zones обновляется еженедельно в 01:00
- datamart_friends_recommendations обновляется ежедневно в 01:00