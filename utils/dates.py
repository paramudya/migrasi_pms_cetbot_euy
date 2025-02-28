import calendar
from datetime import date, datetime


def mtds_generate(last_available_date):
    """
    Generate list containing:
    1. All end of months from January up to last month
    2. Last available date if not end of month

    Args:
        last_available_date: string in format 'YYYY-MM-DD' or datetime object

    Returns:
        list: Date strings in format ['YYYY-MM-DD', ...] including all EoMs and last date
    """
    # Convert string to datetime if needed
    if isinstance(last_available_date, str):
        current_date = datetime.strptime(last_available_date, '%Y-%m-%d')
    else:
        current_date = last_available_date

    year = current_date.year
    month = current_date.month
    day = current_date.day

    result_dates = []

    for m in range(1, month):
        eom = date(year, m, calendar.monthrange(year, m)[1])
        result_dates.append(eom.strftime('%Y-%m-%d'))

    last_day_of_month = calendar.monthrange(year, month)[1]
    if day == last_day_of_month:
        result_dates.append(date(year, month, day).strftime('%Y-%m-%d'))
    else:
        result_dates.append(last_available_date if isinstance(last_available_date, str)
                          else last_available_date.strftime('%Y-%m-%d'))

    return result_dates
