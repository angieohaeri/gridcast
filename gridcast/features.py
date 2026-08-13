import holidays
import numpy as np
import pandas as pd
import typer

app = typer.Typer()


def cyclical_features(df: pd.DataFrame, time_col: str="time", tz: str = "US/Eastern") -> pd.DataFrame:
      local_time = df[time_col].dt.tz_convert(tz)
      hour = local_time.dt.hour
      month = local_time.dt.month
      year = local_time.dt.year

      df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
      df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
      df["month_sin"] = np.sin(2 * np.pi * (month - 1) / 12)
      df["month_cos"] = np.cos(2 * np.pi * (month - 1) / 12)
      df['year'] = year

      return df

pjm_holidays = {
    "New Year's Day",
    "Memorial Day",
    "Independence Day",
    "Labor Day",
    "Thanksgiving Day",
    "Christmas Day"}

def holiday_flag(df: pd.DataFrame, time_col: str = "time", tz: str = "US/Eastern") -> pd.DataFrame:
    # time is stored in UTC; the load drop follows the local calendar day
    local_date = df[time_col].dt.tz_convert(tz).dt.date
    us_holidays = holidays.US(years=range(local_date.min().year, local_date.max().year + 1))

    # "(observed)" rows are the weekday the closure actually lands on - keep both
    holiday_dates = {date for date, name in us_holidays.items()
                    if name.removesuffix(" (observed)") in pjm_holidays}
    
    df["is_holiday"] = local_date.isin(holiday_dates)

    return df

def weekend_flag(df: pd.DataFrame, time_col: str = "time", tz: str = "US/Eastern") -> pd.DataFrame:
    # time is stored in UTC; the load drop follows the local calendar day
    local_date = df[time_col].dt.tz_convert(tz).dt.date
    df["is_weekend"] = pd.to_datetime(local_date).dt.dayofweek >= 5

    return df

def degree_day_features(df: pd.DataFrame, temp_col: str = "temperature") -> pd.DataFrame:
    # base is 65F (18.33C), the standard utility HDD/CDD reference temp; weather is in Celsius
    base_c = 18.33
    df["hdd"] = (base_c - df[temp_col]).clip(lower=0)
    df["cdd"] = (df[temp_col] - base_c).clip(lower=0)

    return df

def peak_hour_flag(df: pd.DataFrame, time_col: str = "time", tz: str = "US/Eastern") -> pd.DataFrame:
    local_hour = df[time_col].dt.tz_convert(tz).dt.hour
    df["is_peak_hour"] = local_hour.between(14, 18)

    return df

# PJM market on-peak: HE 0700-2300 Mon-Fri, excluding NERC holidays - a settlement/pricing
# convention, not the physical demand peak (see peak_hour_flag for that).
# def market_on_peak_flag(df: pd.DataFrame, time_col: str = "time", tz: str = "US/Eastern") -> pd.DataFrame:
#     local_time = df[time_col].dt.tz_convert(tz)
#     is_weekday = local_time.dt.dayofweek < 5
#     us_holidays = holidays.US(years=range(local_time.dt.year.min(), local_time.dt.year.max() + 1))

#     holiday_dates = {date for date, name in us_holidays.items()
#                      if name.removesuffix(" (observed)") in pjm_holidays}

#     is_pjm_holiday = local_time.dt.date.isin(holiday_dates)
#     df["is_market_on_peak"] = local_time.dt.hour.between(7, 22) & is_weekday & ~is_pjm_holiday

#     return df

def drop_time(df: pd.DataFrame, time_col: str = "time") -> pd.DataFrame:
    df.drop(columns=[time_col], inplace=True)
    return df

def features(df: pd.DataFrame, time_col: str = 'time', tz: str = "US/Eastern", drop_time_col=True):
    df.sort_values(by=[time_col], inplace=True)
    df = holiday_flag(df, time_col, tz)
    df = weekend_flag(df, time_col, tz)     
    df = degree_day_features(df)            
    df = peak_hour_flag(df, time_col, tz)   
    df = cyclical_features(df, time_col, tz)
    # df = market_on_peak_flag(df, time_col, tz)
    if drop_time_col is True:
        df = drop_time(df, time_col)
    return df

