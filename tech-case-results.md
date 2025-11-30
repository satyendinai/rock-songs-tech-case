# Rock Songs Tech Case – Antwoorden

## 1. Raw data in de cloud opslaan, opschonen en in een database zetten
**Script:** [clean and store rock songs data.py](https://github.com/satyendinai/rock-songs-tech-case/blob/c8383af94c39993e2c26c45b3141f4e869b4a68b/clean_data/clean%20and%20store%20rock%20songs%20data.py)

### Wat ik heb gedaan:
- **Raw data uploaden naar de cloud:**
  - Het bestand `rock-songs.csv` geüpload naar een S3 bucket in AWS.
- **Opschonen van het bestand:**
  - Een AWS Glue script gemaakt om het bestand uit S3 in te laden.
  - Data cleaning uitgevoerd volgens de stappen in **3a**.
  - Opgeschoonde data weggeschreven naar S3 als een nieuw CSV-bestand.
- **In een database zetten:**
  - Een Glue Crawler gebruikt om de opgeschoonde data (CSV in S3) te crawlen en op te slaan in een tabel in Athena.
  - Alle SQL queries hieronder zijn uitgevoerd op deze Athena-tabel.

### Ook geprobeerd in Databricks:
- Bestand geüpload naar Databricks.
- Data cleaning gelukt volgens stappen in **3a**.
- Kon geen connectie maken met S3 (waarschijnlijk door Free Edition).
- Daarom niet verder gegaan met Databricks.

---

## 2a. Top 10 meest afgespeelde artiesten (gesorteerd op het meest afgespeeld)
**Doel:** Lijst van de 10 meest afgespeelde artiesten, idealiter op basis van totale afspeeltijd.

### Uitgevoerd:
#### SQL Code:
```sql
-- Top 10 meest afgespeelde artiesten op basis van aantal plays
SELECT raw_artist, COUNT(*) AS times_played
FROM "rock-songs-db"."rock-songscleaned_rock_songs"
GROUP BY raw_artist
ORDER BY times_played DESC
LIMIT 10;
```

**Noot:** Ik wilde een API gebruiken om de duur van elk liedje op te halen en die op te tellen per artiest, maar kreeg geen goede responses van de API.

### Extra:
Berekening van airplayduur op basis van tijdsverschil:
#### SQL Code:
```sql
SELECT RAW_ARTIST, SUM(airplay_duration) AS total_airplay
FROM (
    SELECT RAW_ARTIST, RAW_SONG, TIME,
        date_diff('second', TIME, LEAD(TIME) OVER (PARTITION BY CALLSIGN ORDER BY TIME)) AS airplay_duration
    FROM "rock-songs-db"."rock-songscleaned_rock_songs"
)
GROUP BY RAW_ARTIST
ORDER BY total_airplay DESC
LIMIT 10;
```
Top 10 meest afgespeelde liederen:
```sql
SELECT raw_artist, COUNT(*) AS times_played
FROM "rock-songs-db"."rock-songscleaned_rock_songs"
GROUP BY raw_artist
ORDER BY times_played DESC
LIMIT 10;
```
---

## 2b. Top 10 minst afgespeelde artiesten
#### SQL Code:
```sql
SELECT raw_artist, COUNT(*) AS times_played
FROM "rock-songs-db"."rock-songscleaned_rock_songs"
GROUP BY raw_artist
ORDER BY times_played ASC
LIMIT 10;
```

**Noot:** Zelfde beperking voor trackduur (API werkte niet).

---

## 3a. Dataset opschonen zodat elke artiest en combinatie artiest + nummer slechts in één format voorkomt

### Data cleaning stappen:

**I. Rows met 'misaligned columns' apart behandeld (rows met data in kolom _c7; data steekt uit naar rechts van kolom First?)**
1. Data voor kolommen gegenereerd adhv patronen in kolommen die goed lijken door:
    - a. De data in een script onveranderd te gebruiken, zoals in het bestand, zonder delimiters.
    - b. Data voor kolom TIME gehaald door in de rows te zoeken naar waardes tussen 2 `;` met een epoch patroon (10 cijfers achter elkaar) en dat in kolom TIME te zetten.
    - c. Data voor kolom CALLSIGN gehaald door in de rows te zoeken naar waardes tussen 2 `;` met 3 of 4 hoofdletters en dat in kolom CALLSIGN te zetten.
    - d. Data voor kolom COMBINED gehaald door in de rows te zoeken naar waardes tussen 2 `;` met "[tekst] by [tekst]" en dat in kolom COMBINED te zetten.
    - e. Data voor kolom RAW_SONG gehaald door in de gemaakte kolom COMBINED de tekst te pakken voor de laatste " by ".
    - f. Data voor kolom RAW_ARTIST gehaald door in de gemaakte kolom COMBINED de tekst te pakken na de laatste " by ".
    - g. Data voor kolom First? gehaald door in de rows te zoeken naar waardes tussen 2 `;` die 0 of 1 zijn en dat in kolom First? te zetten.
2. De originele kolommen _c7 tot en met _c13 verwijderd.
3. De volgorde van de kolommen gedefinieerd als: TIME, CALLSIGN, COMBINED, RAW_SONG, RAW_ARTIST, First?.

**II. Rows met correcte kolommen behandeld (rows zonder data in kolom _c7; data steekt niet uit naar rechts van kolom First?)**
1. De kolommen hernoemd naar: TIME, CALLSIGN, COMBINED, RAW_SONG, RAW_ARTIST, First?.
    - a. Data voor kolom RAW_SONG gehaald door in de bestaande kolom COMBINED de tekst te pakken voor de laatste " by ".
    - b. Data voor kolom RAW_ARTIST gehaald door in de bestaande kolom COMBINED de tekst te pakken na de laatste " by ".

**III. De twee datasets samengevoegd tot één dataset met correcte kolommen en datatypes.**

---

## 3b. Een lijst van alle unieke artiesten die zijn afgespeeld
#### SQL Code:
```sql
SELECT DISTINCT raw_artist
FROM "rock-songscleaned_rock_songs"
ORDER BY raw_artist;
```

---

## 3c. Aantal artiesten in de dataset
#### SQL Code:
```sql
SELECT COUNT(DISTINCT raw_artist) AS total_artists
FROM "rock-songscleaned_rock_songs";
```

---

## 4. Van iedere artiest in de dataset een foto laten zien
**Script:** [get and store artist image url from spotify api.py](https://github.com/satyendinai/rock-songs-tech-case/blob/c8383af94c39993e2c26c45b3141f4e869b4a68b/spotify_image_url/get%20and%20store%20artist%20image%20url%20from%20spotify%20api.py)

### ✅ Wat ik heb gedaan:
1. Een script gemaakt in AWS Glue om een lijst van unieke artiesten op te halen uit de opgeschoonde dataset in S3.
2. Een connectie gemaakt met de Spotify API om artiestenfoto-URL op te halen.
3. De foto-URL toegevoegd aan de lijst van unieke artiesten.
4. Met een crawler de nieuwe dataset met artiesten en foto-URL gecrawld en opgeslagen in een nieuwe tabel in Athena.

---

## 5. Wat kan ik nog meer doen met de data

### Mogelijke analyses:
1. **Meest afgespeelde liederen per callsign:**
```sql
SELECT callsign,
       raw_song,
       raw_artist,
       play_count
FROM (
    SELECT callsign,
           raw_song,
           raw_artist,
           COUNT(*) AS play_count,
           ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY COUNT(*) DESC) AS rank
    FROM "rock-songscleaned_rock_songs"
    GROUP BY callsign, raw_song, raw_artist
) ranked
WHERE rank = 1
ORDER BY callsign;
```

2. **Top 10 meest actieve callsigns:**
```sql
SELECT callsign,
       COUNT(*) AS total_plays
FROM "rock-songscleaned_rock_songs"
GROUP BY callsign
ORDER BY total_plays DESC
LIMIT 10;
```

3. **Hoelaat (welk uur) worden de meeste liederen afgespeeld:**
```sql
SELECT hour(time) AS play_hour,
       COUNT(*) AS play_count
FROM "rock-songscleaned_rock_songs"
GROUP BY hour(time)
ORDER BY play_count DESC;
```

4. **Op welke dagen van de week worden de meeste liederen afgespeeld:**
```sql
SELECT day_of_week(time) AS day_number,
       CASE day_of_week(time)
           WHEN 1 THEN 'Monday'
           WHEN 2 THEN 'Tuesday'
           WHEN 3 THEN 'Wednesday'
           WHEN 4 THEN 'Thursday'
           WHEN 5 THEN 'Friday'
           WHEN 6 THEN 'Saturday'
           WHEN 7 THEN 'Sunday'
       END AS day_name,
       COUNT(*) AS play_count
FROM "rock-songscleaned_rock_songs"
GROUP BY day_of_week(time)
ORDER BY play_count DESC;
```

### Andere mogelijkheden:
- **SQL:**
    - Meest afgespeelde liederen per dag (van het jaar ipv van de week).
    - Meest afgespeelde artiesten per dag (van het jaar en van de week).
    - Meest actieve callsigns per dag.
    - Meest actieve callsigns per uur.
    - Meest actieve dagen per callsign.

- **Python:**
    - Spotify API gebruiken om meer informatie over de liederen op te halen (album, releasedatum, genre, populariteit etc).
    - FCC API gebruiken om meer informatie over de callsigns op te halen (locatie, eigenaar, type licentie etc).
    - NOAA of OpenWeather API gebruiken om weergegevens te koppelen aan de tijdstippen van afgespeelde liederen (in welk weertype worden welke liederen afgespeeld?).

---

### Toegepaste technieken (ETL/Data Lake/CICD):
- AWS S3 voor opslag van raw en opgeschoonde data.
- AWS Glue voor data cleaning en transformatie (ETL dmv Python Scripts).
- AWS Athena als database en voor SQL queries.
- AWS Crawler voor het maken van tabellen in Athena.
- CICD/Version Control dmv GitHub voor versiebeheer en documentatie.

**Wat beter kan:** Alle losse scripts samenvoegen in een enkele geautomatiseerde ETL pipeline.

### Performance:
- Data cleaning en transformatie gedaan in AWS Glue Scripts (serverless, schaalbaar).
- SQL queries uitgevoerd in AWS Athena (serverless, schaalbaar).

### Code style:
- Duidelijke en beschrijvende namen voor variabelen en functies.
- Commentaar toegevoegd om de stappen en logica uit te leggen.
- Modulariteit: niet heel modulair gemaakt omdat het om een eenmalige data cleaning taak ging en tijdsbeperkingen.
- Consistente inspringing en opmaak.

### Documentatie:
- Deze antwoorden in **tech-case-results.md**.
- README in GitHub repository: [README.md](https://github.com/satyendinai/rock-songs-tech-case/blob/main/README.md).
- Technical Documentation in GitHub repository: [Technical Documentation.md](https://github.com/satyendinai/rock-songs-tech-case/blob/main/Technical%20Documentation.md).
- Data cleaning script in GitHub repository.
- Extra script voor ophalen artiestenfoto-URL in GitHub repository.
