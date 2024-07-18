SELECT weather.date as current_date,
    weather.temp, 
    weather.feels_like,
    weather.sunrise,
    weather.sunset,
    exchange_rates.mid as eur_to_pln,
    history_facts.date_fact,
    CONCAT(daily_word.word, ': ', daily_word.description) as daily_word,
    bored_activity.activity
FROM morning_report.weather
  LEFT JOIN morning_report.exchange_rates
    ON weather.date = exchange_rates.date
  LEFT JOIN morning_report.history_facts
    ON weather.date = history_facts.date       
  LEFT JOIN morning_report.daily_word
    ON weather.date = daily_word.date    
  LEFT JOIN morning_report.bored_activity
    ON weather.date = bored_activity.date    
WHERE 
  CAST(weather.date as DATE) = CURRENT_DATE();
