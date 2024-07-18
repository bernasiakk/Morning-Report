-- weather (sample API: https://api.openweathermap.org/data/2.5/weather?units=metric&q=warszawa&appid=***)
CREATE TABLE morning_report.weather(
    id STRING,
    city STRING,
    main STRING,
    description STRING,
    temp DECIMAL,
    feels_like DECIMAL,
    sunrise DATETIME, -- adjusted for timezone
    sunset DATETIME, -- adjusted for timezone
    date DATE DEFAULT CURRENT_DATE(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- exchange_rates (sample API: http://api.nbp.pl/api/exchangerates/rates/a/eur/today/)
CREATE TABLE morning_report.exchange_rates(
    id STRING,
    currency STRING,
    code STRING(3),
    mid DECIMAL(7,4),
    effective_date DATE,
    date DATE DEFAULT CURRENT_DATE(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- history_facts (sample API: http://numbersapi.com/7/4/date)
CREATE TABLE morning_report.history_facts(
    id STRING,
    date_fact STRING,
    date DATE DEFAULT CURRENT_DATE(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- daily_word (sample API: https://wordsmith.org/awad/rss1.xml)
CREATE TABLE morning_report.daily_word(
    id STRING,
    word STRING,
    description STRING,
    date DATE DEFAULT CURRENT_DATE(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- bored_activity (sample API: https://bored-api.appbrewery.com/random)
CREATE TABLE morning_report.bored_activity(
    id STRING,
    activity STRING,
    date DATE DEFAULT CURRENT_DATE(),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
