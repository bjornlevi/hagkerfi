# Hagkerfi data sources

This project keeps all raw inputs under `data/` (landing/bronze) so you can refresh or replace them manually if downloads fail. Key sources and where to place them:

- **National budget (fjarlog_2026.xlsx)** – https://www.stjornarradid.is/library/02-Rit--skyrslur-og-skrar/T%C3%B6flur%20%C3%AD%20fj%C3%A1rlagafrumvarpi%202026%20-%20001.xlsx (put in `data/landing/`).
- **Municipal accounts pivot (arsreikningar_sveitarfelaga.xlsx)** – https://samband-islenskra-sveitarfelaga.cdn.prismic.io/samband-islenskra-sveitarfelaga/aRWnC7pReVYa4azt_Net_%C3%81rsreikningar_Pivot.xlsx (put in `data/landing/`).
- **Municipal income tax rates (utsvar_sveitarfelaga.xls)** – source URL not configured; drop the file manually into `data/landing/`.
- **Population distribution (population_distribution.json)** – Hagstofa API: https://px.hagstofa.is/pxis/api/v1/is/Ibuar/mannfjoldi/1_yfirlit/Yfirlit_mannfjolda/MAN00101.px. Saved as JSON in `data/landing/`.
- **Income by gender/age (gender_age_income.json)** – Hagstofa API: https://px.hagstofa.is/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/TEK01001.px. Saved as JSON in `data/landing/`.
- **Employment by age/gender (employment.json)** – Hagstofa API: https://px.hagstofa.is/pxis/api/v1/is/Samfelag/vinnumarkadur/vinnuaflskraargogn/VIN10001.px. Saved as JSON in `data/landing/`.
- **Property value estimates (property_value_estimates.csv)** – HMS dashboard: https://hms.is/gogn-og-maelabord/maelabordfasteignaskra/heildar-fasteigna--og-brunabotamat. Place the CSV in `data/` (pipeline copies it to bronze).
- **Property tax table (property_tax_amount.xlsx)** – Samband: https://www.samband.is/tekjustofnar. Place in `data/` (pipeline converts to `data/bronze/property_tax_amount.csv`).

Pipelines:
- `pipelines/landing_download.py` downloads the API/static sources into `data/landing/`.
- `pipelines/mint_bronze.py` copies/cleans landing files into `data/bronze/` (including the property value/tax files if present).
- `pipelines/mint_silver.py` loads all bronze CSVs into `data/silver.db`.
