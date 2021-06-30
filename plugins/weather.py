import requests
import pandas as pd
from API import api_key, api_url

def weather_forecast():
    # Set up parameter dictionary according to documentation
    params = {"q": "Columbus", 
            "days": "1"}

    # # Set up header dictionary w/ API key according to documentation
    headers = {"key": api_key}

    # Call the API
    response = requests.get(api_url,
                            params=params,
                            headers=headers)

    weather_forecast = response.json()

    # Cleaning and loading location data
    wf_location = pd.json_normalize(weather_forecast["location"])
    forecast_region = pd.DataFrame(wf_location[["name","region", "country"]])
    forecast_region = forecast_region.rename(columns={"name": "City","region": "Region", "country": "Country"})

    # Cleaning and loading Realtime weather data
    wf_current = pd.json_normalize(weather_forecast["current"])
    forecast_current = pd.DataFrame(wf_current[["temp_f", "feelslike_f", "cloud"]])
    forecast_current = forecast_current.rename(columns={"temp_f": "Current Temperature(°F)", "feelslike_f": "Feels Like(°F)", "cloud": "Cloud Cover(%)"})

    # Cleaning and loading daily forecast data
    wf_forecast = pd.json_normalize(weather_forecast["forecast"], record_path="forecastday")
    forecast_day = pd.DataFrame(wf_forecast[["date", "day.maxtemp_f", "day.mintemp_f", "day.avgtemp_f"]])
    forecast_day = forecast_day.rename(columns={"date": "Date", "day.maxtemp_f": "Highest Temperature(°F)", "day.mintemp_f": "Lowest Temperature(°F)", "day.avgtemp_f": "Average Temperature(°F)"})

    # Cleaning and loading hourly forecast data
    def hf_data():
        data = pd.DataFrame()
        for a in range(24) :
            b = pd.DataFrame(wf_forecast.hour.values.tolist())[a]
            c = pd.json_normalize(b)
            appended = data.append(c, ignore_index=False)
            data = pd.DataFrame(appended)
        return data
    data_hour = hf_data()

    forecast_hour = pd.DataFrame(data_hour[["time", "temp_f", "cloud","feelslike_f", "will_it_rain", "will_it_snow", "chance_of_rain", "chance_of_snow"]])
    forecast_hour['time'] = forecast_hour['time'].str[10:]
    forecast_hour['will_it_rain'] = forecast_hour['will_it_rain'].replace([0, 1], ["No", "Yes"])
    forecast_hour['will_it_snow'] = forecast_hour['will_it_snow'].replace([0, 1], ["No", "Yes"])
    forecast_hour = forecast_hour.rename(columns={"time": "Time", "temp_f": "Temperature(°F)", "cloud": "Cloud Cover(%)","feelslike_f": "Feels Like(°F)", "will_it_rain": "Rain", "will_it_snow": "Snow", "chance_of_rain": "Chance of Rain(%)", "chance_of_snow": "Chance of Snow(%)"})
    
    # Load weather data to html
    with open("weather_forecast.html", 'w') as _file:
        _file.write(forecast_region.to_html() + "\n\n" + forecast_current.to_html() + "\n\n" + forecast_day.to_html()+ "\n\n" + forecast_hour.to_html())
