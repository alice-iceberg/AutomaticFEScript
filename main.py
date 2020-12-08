# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This script is for Feature Extraction of StressSensor study                           #
# First, put all the users raw data files downloaded from dashboard to one folder       #
# Please, run the following command to execute it:                                      #
# python main.py C:/StressSensor/Data user1@smth.com,user2@smth.com,user3@smth.com      #
# 1st arg: python                                                                       #
# 2nd arg: script filename (main.py)                                                    #
# 3rd arg: url for location of all raw data files                                       #
# 4th arg: list of user email for whom you want to extract features, separated by ","   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


import statistics
import urllib.request
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

mood_scores = []
food_scores = []
sleep_scores = []
physical_activity_scores = []
social_activity_scores = []

NUMBER_OF_EMA = 6
LOCATION_HOME = "HOME"
LOCATION_LIBRARY = "LIBRARY"
LOCATION_UNIVERSITY = "UNIV"

UNLOCK_DURATION = "UNLOCK_DURATION"
CALLS = "CALLS"
ACTIVITY_TRANSITION = "ACTIVITY_TRANSITION"
ACTIVITY_RECOGNITION = "ACTIVITY_RECOGNITION"
AUDIO_LOUDNESS = "AUDIO_LOUDNESS"
TOTAL_DIST_COVERED = "TOTAL_DIST_COVERED"
MAX_DIST_TWO_LOCATIONS = "MAX_DIST_TWO_LOCATIONS"
RADIUS_OF_GYRATION = "RADIUS_OF_GYRATION"
MAX_DIST_FROM_HOME = "MAX_DIST_FROM_HOME"
NUM_OF_DIF_PLACES = "NUM_OF_DIF_PLACES"
GEOFENCE = "GEOFENCE"
SCREEN_ON_OFF = "SCREEN_ON_OFF"
APPLICATION_USAGE = "APPLICATION_USAGE"
SURVEY_EMA = "SURVEY_EMA"

APP_PCKG_TOCATEGORY_MAP_FILENAME = "package_to_category_map.csv"

pckg_to_cat_map = {}
cat_list = pd.read_csv('Cat_group.csv')


def in_range(number, start, end):
    if start <= number <= end:
        return True
    else:
        return False


def get_filename_from_data_src(filenames, data_src, username):
    for filename in filenames:
        if username in filename and data_src in filename:
            return filename


def from_timestamp_to_month(timestamp):
    timestamp = int(timestamp)
    dt = datetime.fromtimestamp(timestamp / 1000.0)
    month = dt.month
    return month


def from_timestamp_to_day(timestamp):
    timestamp = int(timestamp)
    dt = datetime.fromtimestamp(timestamp / 1000.0)
    day = dt.day
    return day


def from_timestamp_to_ema_order(timestamp):
    # EMA1 : 22:00:00 - 09:59:59
    # EMA2 : 10:00:00 - 13:59:59
    # EMA3 : 14:00:00 - 17:59:59
    # EMA4 : 18:00:00 - 21:59:59

    timestamp = int(timestamp)
    ema_order = 0

    dt = datetime.fromtimestamp(timestamp / 1000.0)
    if 0 <= dt.hour < 10 or 22 <= dt.hour <= 23:
        ema_order = 1
    elif 10 <= dt.hour < 14:
        ema_order = 2
    elif 14 <= dt.hour < 18:
        ema_order = 3
    elif 18 <= dt.hour < 22:
        ema_order = 4

    return ema_order


def get_google_category(app_package):
    url = "https://play.google.com/store/apps/details?id=" + app_package
    grouped_category = ""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            source = response.read()

        soup = BeautifulSoup(source, 'html.parser')
        table = soup.find_all("a", {'itemprop': 'genre'})

        genre = table[0].get_text()

        grouped = cat_list[cat_list['App Category'] == genre]['Grouped Category'].values

        if len(grouped) > 0:
            grouped_category = grouped[0]
        else:
            grouped_category = 'NotMapped'
    except Exception:
        grouped_category = 'Unknown or Background'

    finally:
        return grouped_category


def get_unlock_result(filename, start_time, end_time, username_id):
    result = {
        "duration": 0,
        "number": 0
    }
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]

    for line_unlock in rows.itertuples(index=False):
        start = line_unlock.timestamp_start
        end = line_unlock.timestamp_end
        duration = line_unlock.duration
        if in_range(int(start) / 1000, start_time, end_time) and in_range(int(end) / 1000, start_time, end_time):
            result['duration'] += int(duration)
            result['number'] += 1

    if result['number'] == 0:
        result['duration'] = 0
        result['number'] = 0

    return result


def get_unlock_duration_at_location(filename_geofence, filename_unlock, start_time, end_time, location_name,
                                    username_id):
    result = {
        "duration": 0,
        "number": 0
    }
    geofence_rows = pd.read_csv(filename_geofence)
    geofence_rows = geofence_rows.loc[geofence_rows['username_id'] == username_id]

    for line_geofence in geofence_rows.itertuples(index=False):
        enter_time = line_geofence.timestamp_enter
        exit_time = line_geofence.timestamp_exit
        location = line_geofence.location

        if in_range(int(enter_time) / 1000, start_time, end_time) and location == location_name:
            unlock_rows = pd.read_csv(filename_unlock)
            unlock_rows = unlock_rows.loc[unlock_rows['username_id'] == username_id]

            for line_unlock in unlock_rows.itertuples(index=False):
                start = line_unlock.timestamp_start
                end = line_unlock.timestamp_end
                duration = line_unlock.duration
                if in_range(int(start) / 1000, int(enter_time) / 1000, int(exit_time) / 1000) and in_range(
                        int(end) / 1000, int(enter_time) / 1000, int(exit_time) / 1000):
                    result['duration'] += int(duration)
                    result['number'] += 1

    if result['number'] == 0:
        result['duration'] = 0
        result['number'] = 0

    return result


def get_total_distance(filename, start_time, end_time, username_id):
    result = 0.0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        distance = row.value
        if in_range(int(start) / 1000, start_time, end_time):
            result = float(distance)

    return result


def get_std_of_displacement(filename, start_time, end_time, username_id):
    result = 0.0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        value = row.value
        if in_range(int(start) / 1000, start_time, end_time):
            result = float(value)

    return result


def get_steps(filename, start_time, end_time, username_id):
    result = 0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        timestamp = row.timestamp
        if in_range(int(timestamp) / 1000, start_time, end_time):
            result += 1

    return result


def get_sig_motion(filename, start_time, end_time, username_id):
    result = 0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        timestamp = row.timestamp
        if in_range(int(timestamp) / 1000, start_time, end_time):
            result += 1

    return result


def get_radius_of_gyration(filename, start_time, end_time, username_id):
    result = 0.0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        value = row.value
        if in_range(int(start) / 1000, start_time, end_time):
            result = float(value)

    return result


def get_phonecall(filename, start_time, end_time, username_id):
    result = {
        "in_duration": 0,
        "out_duration": 0,
        "in_number": 0,
        "out_number": 0
    }

    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        call_type = row.call_type
        duration = row.duration
        if in_range(int(end) / 1000, start_time, end_time):
            if call_type == "IN":
                result["in_duration"] += int(duration)
                result["in_number"] += 1
            elif call_type == "OUT":
                result["out_duration"] += int(duration)
                result["out_number"] += 1

    if result["in_number"] == 0:
        result["in_duration"] = 0
        result["in_number"] = 0

    if result["out_number"] == 0:
        result["out_duration"] = 0
        result["out_number"] = 0

    return result


def get_num_of_dif_places(filename, start_time, end_time, username_id):
    result = 0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        value = row.value
        if in_range(int(start) / 1000, start_time, end_time):
            result = int(value)

    return result


def get_max_dist_two_locations(filename, start_time, end_time, username_id):
    result = 0.0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        value = row.value
        if in_range(int(start) / 1000, start_time, end_time):
            result = float(value)

    return result


def get_max_dist_home(filename, start_time, end_time, username_id):
    result = 0.0
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.timestamp_start
        end = row.timestamp_end
        value = row.value
        if in_range(int(start) / 1000, start_time, end_time):
            result = float(value)

    return result


def get_light(filename, start_time, end_time, username_id):
    result = {
        'min': 0,
        'max': 0,
        'avg': 0
    }
    light_data = []
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        timestamp = row.timestamp
        value = row.value
        if in_range(int(timestamp) / 1000, start_time, end_time):
            light_data.append(int(value))

    if light_data.__len__() > 0:
        result['min'] = min(light_data)
        result['max'] = max(light_data)
        result['avg'] = statistics.mean(light_data)
    else:
        result['min'] = 0
        result['max'] = 0
        result['avg'] = 0

    return result


def get_hrm(filename, start_time, end_time, username_id):
    result = {
        'min': 0,
        'max': 0,
        'avg': 0
    }
    hrm_data = []
    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        timestamp = row.timestamp
        value = row.value
        if in_range(int(timestamp) / 1000, start_time, end_time):
            hrm_data.append(int(value))

    if hrm_data.__len__() > 0:
        result['min'] = min(hrm_data)
        result['max'] = max(hrm_data)
        result['avg'] = statistics.mean(hrm_data)
    else:
        result['min'] = 70
        result['max'] = 70
        result['avg'] = 70

    return result


def get_num_of_dif_activities(filename, start_time, end_time, username_id):
    result = {
        "still": 0,
        "walking": 0,
        "running": 0,
        "on_bicycle": 0,
        "in_vehicle": 0,
        "on_foot": 0,
        "tilting": 0,
        "unknown": 0
    }

    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        timestamp = row.timestamp
        activity_type = row.activity_type
        if in_range(int(timestamp) / 1000, start_time, end_time):

            if activity_type == 'STILL':
                result['still'] += 1
            elif activity_type == 'WALKING':
                result['walking'] += 1
            elif activity_type == 'RUNNING':
                result['running'] += 1
            elif activity_type == 'ON_BICYCLE':
                result['on_bicycle'] += 1
            elif activity_type == 'IN_VEHICLE':
                result['in_vehicle'] += 1
            elif activity_type == 'ON_FOOT':
                result['on_foot'] += 1
            elif activity_type == 'TILTING':
                result['tilting'] += 1
            elif activity_type == 'UNKNOWN':
                result['unknown'] += 1

    if result['still'] == 0:
        result['still'] = 0
    if result['walking'] == 0:
        result['walking'] = 0
    if result['running'] == 0:
        result['running'] = 0
    if result['on_bicycle'] == 0:
        result['on_bicycle'] = 0
    if result['in_vehicle'] == 0:
        result['in_vehicle'] = 0
    if result['on_foot'] == 0:
        result['on_foot'] = 0
    if result['tilting'] == 0:
        result['tilting'] = 0
    if result['unknown'] == 0:
        result['unknown'] = 0

    return result


def get_app_category_usage(filename, start_time, end_time, username_id):
    result_duration = {
        "Entertainment & Music": 0,
        "Utilities": 0,
        "Shopping": 0,
        "Games & Comics": 0,
        "Others": 0,
        "Health & Wellness": 0,
        "Social & Communication": 0,
        "Education": 0,
        "Travel": 0,
        "Art & Design & Photo": 0,
        "News & Magazine": 0,
        "Food & Drink": 0,
        "Unknown & Background": 0
    }

    result_frequency = {
        "Entertainment & Music": 0,
        "Utilities": 0,
        "Shopping": 0,
        "Games & Comics": 0,
        "Others": 0,
        "Health & Wellness": 0,
        "Social & Communication": 0,
        "Education": 0,
        "Travel": 0,
        "Art & Design & Photo": 0,
        "News & Magazine": 0,
        "Food & Drink": 0,
        "Unknown & Background": 0
    }

    rows = pd.read_csv(filename)
    rows = rows.loc[rows['username_id'] == username_id]
    for row in rows.itertuples(index=False):
        start = row.start_timestamp
        end = row.end_timestamp
        pckg_name = row.package_name
        duration = int(end) - int(start)
        if in_range(int(start), start_time, end_time) and in_range(int(end), start_time, end_time) and duration > 0:
            if pckg_name in pckg_to_cat_map:
                category = pckg_to_cat_map[pckg_name]
            else:
                category = get_google_category(pckg_name)
                pckg_to_cat_map[pckg_name] = category

            if category == "Entertainment & Music":
                result_duration['Entertainment & Music'] += duration
                result_frequency['Entertainment & Music'] += 1
            elif category == "Utilities":
                result_duration['Utilities'] += duration
                result_frequency['Utilities'] += 1
            elif category == "Shopping":
                result_duration['Shopping'] += duration
                result_frequency['Shopping'] += 1
            elif category == "Games & Comics":
                result_duration['Games & Comics'] += duration
                result_frequency['Games & Comics'] += 1
            elif category == "Others":
                result_duration['Others'] += duration
                result_frequency['Others'] += 1
            elif category == "Health & Wellness":
                result_duration['Health & Wellness'] += duration
                result_frequency['Health & Wellness'] += 1
            elif category == "Social & Communication":
                result_duration['Social & Communication'] += duration
                result_frequency['Social & Communication'] += 1
            elif category == "Education":
                result_duration['Education'] += duration
                result_frequency['Education'] += 1
            elif category == "Travel":
                result_duration['Travel'] += duration
                result_frequency['Travel'] += 1
            elif category == "Art & Design & Photo":
                result_duration['Art & Design & Photo'] += duration
                result_frequency['Art & Design & Photo'] += 1
            elif category == "News & Magazine":
                result_duration['News & Magazine'] += duration
                result_frequency['News & Magazine'] += 1
            elif category == "Food & Drink":
                result_duration['Food & Drink'] += duration
                result_frequency['Food & Drink'] += 1
            elif category == "Unknown & Background":
                result_duration['Unknown & Background'] += duration
                result_frequency['Unknown & Background'] += 1

    return result_duration, result_frequency


def drop_no_ema_records():
    filename_save = 'ema_responses_filtered.csv'
    dataframe = pd.read_csv('ema_responses.csv', delimiter=',', header=0, low_memory=False)
    # dataframe = dataframe.sort_values(by=['username_id', 'time_expected'])

    dataframe = dataframe.drop(dataframe[dataframe.time_responded == 0].index)
    dataframe.to_csv(filename_save, index=False)


def sort_dataframe():
    dataframe = pd.read_csv('extracted_features_with_sleep.csv', delimiter=',', header=0, low_memory=False)
    dataframe = dataframe.sort_values(by=['username_id', 'day', 'month', 'ema'])

    dataframe = dataframe.drop(dataframe[dataframe.time_responded == 0].index)
    dataframe.to_csv('extracted_features_sorted.csv', index=False)


def extract_features():
    ema_orders = []
    days = []
    months = []

    try:
        columns = [
            'user_id',
            'day_num',
            'month',
            'ema',
            'phq1',
            'phq2',
            'phq3',
            'phq4',
            'phq5',
            'phq6',
            'phq7',
            'phq8',
            'phq9',
            'unlock_duration',
            'unlock_number',
            'unlock_duration_home',
            'unlock_number_home',
            'unlock_duration_univ',
            'unlock_number_univ',
            'unlock_duration_library',
            'unlock_number_library',
            'total_distance',
            'std_displacement',
            'steps',
            'significant_motion',
            'radius_of_gyration',
            'in_call_duration',
            'in_call_number',
            'out_call_duration',
            'out_call_number',
            'num_of_dif_places',
            'max_dist_btw_two_locations',
            'max_dist_home',
            'light_min',
            'light_max',
            'light_avg',
            'hrm_min',
            'hrm_max',
            'hrm_avg',
            'still_number',
            'walking_number',
            'running_number',
            'on_bicycle_number',
            'in_vehicle_number',
            'on_foot_number',
            'tilting_number',
            'unknown_number',
            'app_entertainment_music_dur',
            'app_utilities_dur',
            'app_shopping_dur',
            'app_games_comics_dur',
            'app_health_wellness_dur',
            'app_social_communication_dur',
            'app_education_dur',
            'app_travel_dur',
            'app_art_design_photo_dur',
            'app_news_magazine_dur',
            'app_food_drink_dur',
            'app_unknown_background_dur',
            'app_others_dur',
            'app_entertainment_music_freq',
            'app_utilities_freq',
            'app_shopping_freq',
            'app_games_comics_freq',
            'app_health_wellness_freq',
            'app_social_communication_freq',
            'app_education_freq',
            'app_travel_freq',
            'app_art_design_photo_freq',
            'app_news_magazine_freq',
            'app_food_drink_freq',
            'app_unknown_background_freq',
            'app_others_freq',
        ]

        header = True
        ema_rows = pd.read_csv('ema_responses_filtered.csv')

        import datetime
        print("Features extraction start time: {}".format(datetime.datetime.now()))

        for idx, row in enumerate(ema_rows.itertuples(index=False)):
            print("Total iterations expected: ", ema_rows.__len__())
            print("Iteration: {}".format(idx + 1))
            print("Iterations start time: {}".format(datetime.datetime.now()))

            ema_orders.append(from_timestamp_to_ema_order(int(row.time_expected)))
            days.append(from_timestamp_to_day(int(row.time_expected)))
            months.append(from_timestamp_to_month(int(row.time_expected)))

            if int(row.day_num) <= 35:
                thirty_mins_constant = 1800
                end_time_decrement = 0
                counter = 0

                while end_time_decrement < 14400:
                    counter += 1
                    end_time = int(row.time_expected)
                    end_time -= end_time_decrement
                    end_time_decrement += 1800  # 30 minutes

                    start_time = end_time - thirty_mins_constant  # 14400sec = 4 hours before each EMA

                    if start_time < 0:
                        continue

                    print("Start time: ", start_time, "End time:", end_time)

                    unlock_data = get_unlock_result(
                        "unlock_duration.csv", start_time,
                        end_time,
                        row.username_id)
                    unlock_at_home_data = get_unlock_duration_at_location(
                        "geofencing.csv",
                        "unlock_duration.csv",
                        start_time,
                        end_time, LOCATION_HOME, row.username_id)
                    unlock_at_univ_data = get_unlock_duration_at_location(
                        "geofencing.csv",
                        "unlock_duration.csv",
                        start_time,
                        end_time, LOCATION_UNIVERSITY, row.username_id)

                    unlock_at_library_data = get_unlock_duration_at_location(
                        "geofencing.csv",
                        "unlock_duration.csv",
                        start_time,
                        end_time, LOCATION_LIBRARY, row.username_id)

                    total_distance_data = get_total_distance(
                        "total_dist_covered.csv",
                        start_time,
                        end_time,
                        row.username_id)

                    std_displacement_data = get_std_of_displacement(
                        "std_of_displacement.csv",
                        start_time,
                        end_time, row.username_id)

                    steps_data = get_steps(
                        "steps.csv",
                        start_time, end_time, row.username_id)

                    sig_motion_data = get_sig_motion(
                        "significant_motion.csv",
                        start_time,
                        end_time,
                        row.username_id)

                    rad_of_gyration_data = get_radius_of_gyration(
                        "radius_of_gyration.csv",
                        start_time, end_time,
                        row.username_id)

                    calls_data = get_phonecall(
                        "phone_calls.csv", start_time,
                        end_time,
                        row.username_id)

                    num_of_dif_places_data = get_num_of_dif_places(
                        "num_of_dif_places.csv",
                        start_time, end_time,
                        row.username_id)

                    max_dist_two_locations_data = get_max_dist_two_locations(
                        "max_dist_two_locations.csv",
                        start_time, end_time, row.username_id)

                    max_dist_home_data = get_max_dist_home(
                        "max_dist_from_home.csv",
                        start_time,
                        end_time,
                        row.username_id)

                    light_data = get_light(
                        "light.csv",
                        start_time, end_time, row.username_id)

                    hrm_data = get_hrm("hrm.csv",
                                       start_time,
                                       end_time, row.username_id)

                    activity_number_data = get_num_of_dif_activities(
                        "activities.csv", start_time,
                        end_time,
                        row.username_id)

                    app_usage_dur_data, app_usage_freq_data = get_app_category_usage(
                        "app_usage.csv",
                        start_time,
                        end_time,
                        row.username_id)

                    day_hour_start = 18
                    day_hour_end = 11
                    date_start = datetime.datetime.fromtimestamp(end_time)
                    date_start = date_start - datetime.timedelta(days=1)
                    date_start = date_start.replace(hour=day_hour_start, minute=0, second=0)
                    date_end = datetime.datetime.fromtimestamp(end_time)
                    date_end = date_end.replace(hour=day_hour_end, minute=0, second=0)

                    data = {
                        'user_id': row.username_id,
                        'day_num': days[len(days) - 1],
                        'month': months[len(months) - 1],
                        'ema': ema_orders[len(ema_orders) - 1],
                        'phq1': row.interest,
                        'phq2': row.mood,
                        'phq3': row.sleep,
                        'phq4': row.fatigue,
                        'phq5': row.weight,
                        'phq6': row.worthlessness,
                        'phq7': row.concentrate,
                        'phq8': row.restlessness,
                        'phq9': row.suicide,
                        'unlock_duration': unlock_data['duration'],
                        'unlock_number': unlock_data['number'],
                        'unlock_duration_home': unlock_at_home_data['duration'],
                        'unlock_number_home': unlock_at_home_data['number'],
                        'unlock_duration_univ': unlock_at_univ_data['duration'],
                        'unlock_number_univ': unlock_at_univ_data['number'],
                        'unlock_duration_library': unlock_at_library_data['duration'],
                        'unlock_number_library': unlock_at_library_data['number'],
                        'total_distance': total_distance_data,
                        'std_displacement': std_displacement_data,
                        'steps': steps_data,
                        'significant_motion': sig_motion_data,
                        'radius_of_gyration': rad_of_gyration_data,
                        'in_call_duration': calls_data['in_duration'],
                        'in_call_number': calls_data['in_number'],
                        'out_call_duration': calls_data['out_duration'],
                        'out_call_number': calls_data['out_number'],
                        'num_of_dif_places': num_of_dif_places_data,
                        'max_dist_btw_two_locations': max_dist_two_locations_data,
                        'max_dist_home': max_dist_home_data,
                        'light_min': light_data['min'],
                        'light_max': light_data['max'],
                        'light_avg': light_data['avg'],
                        'hrm_min': hrm_data['min'],
                        'hrm_max': hrm_data['max'],
                        'hrm_avg': hrm_data['avg'],
                        'still_number': activity_number_data['still'],
                        'walking_number': activity_number_data['walking'],
                        'running_number': activity_number_data['running'],
                        'on_bicycle_number': activity_number_data['on_bicycle'],
                        'in_vehicle_number': activity_number_data['in_vehicle'],
                        'on_foot_number': activity_number_data['on_foot'],
                        'tilting_number': activity_number_data['tilting'],
                        'unknown_number': activity_number_data['unknown'],
                        'app_entertainment_music_dur': app_usage_dur_data['Entertainment & Music'],
                        'app_utilities_dur': app_usage_dur_data['Utilities'],
                        'app_shopping_dur': app_usage_dur_data['Shopping'],
                        'app_games_comics_dur': app_usage_dur_data['Games & Comics'],
                        'app_health_wellness_dur': app_usage_dur_data['Health & Wellness'],
                        'app_social_communication_dur': app_usage_dur_data['Social & Communication'],
                        'app_education_dur': app_usage_dur_data['Education'],
                        'app_travel_dur': app_usage_dur_data['Travel'],
                        'app_art_design_photo_dur': app_usage_dur_data['Art & Design & Photo'],
                        'app_news_magazine_dur': app_usage_dur_data['News & Magazine'],
                        'app_food_drink_dur': app_usage_dur_data['Food & Drink'],
                        'app_unknown_background_dur': app_usage_dur_data['Unknown & Background'],
                        'app_others_dur': app_usage_dur_data['Others'],
                        'app_entertainment_music_freq': app_usage_freq_data['Entertainment & Music'],
                        'app_utilities_freq': app_usage_freq_data['Utilities'],
                        'app_shopping_freq': app_usage_freq_data['Shopping'],
                        'app_games_comics_freq': app_usage_freq_data['Games & Comics'],
                        'app_health_wellness_freq': app_usage_freq_data['Health & Wellness'],
                        'app_social_communication_freq': app_usage_freq_data['Social & Communication'],
                        'app_education_freq': app_usage_freq_data['Education'],
                        'app_travel_freq': app_usage_freq_data['Travel'],
                        'app_art_design_photo_freq': app_usage_freq_data['Art & Design & Photo'],
                        'app_news_magazine_freq': app_usage_freq_data['News & Magazine'],
                        'app_food_drink_freq': app_usage_freq_data['Food & Drink'],
                        'app_unknown_background_freq': app_usage_freq_data['Unknown & Background'],
                        'app_others_freq': app_usage_freq_data['Others'],
                    }

                    df = pd.DataFrame(data, index=[0])
                    df = df[columns]
                    mode = 'w' if header else 'a'

                    df.to_csv('extracted_features.csv',
                              encoding='utf-8', mode=mode, header=header, index=False)
                    header = False

        print("End time: {}".format(datetime.datetime.now()))

    except Exception as e:
        print("Exception: ", e)


def is_sleep_hour_in_range(hour):
    if hour >= 22:
        return True
    elif hour <= 10:
        return True
    else:
        return False


def from_timestamp_to_hour(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000.0)
    hour = dt.hour
    return hour


def fix_days_and_ema_orders():
    dt_features = pd.read_csv('extracted_features.csv')
    dt_ema = pd.read_csv('ema_responses_filtered.csv', delimiter=',', header=0)

    ema_orders = []
    days = []
    months = []

    for index, row in dt_ema.iterrows():

        for i in range(8):
            ema_orders.append(from_timestamp_to_ema_order(int(row.time_expected)))
            days.append(from_timestamp_to_day(int(row.time_expected)))
            months.append(from_timestamp_to_month(int(row.time_expected)))

    dt_features['day_num'] = days
    dt_features['ema'] = ema_orders
    dt_features['month'] = months

    dt_features.to_csv('extracted_features_fixed.csv', index=False)


def get_sleep_duration():
    dataframe = pd.read_csv('unlock_duration.csv', delimiter=',', header=0)
    locked_duration = []
    sleep_duration = []
    users = []
    days = []
    months = []
    prev_username = '0'
    prev_time_end = 0
    prev_day = 0
    prev_month = 0
    sleep_scores = []

    for index, row in dataframe.iterrows():

        if is_sleep_hour_in_range(from_timestamp_to_hour(int(row.timestamp_end))):
            if prev_username == row.username_id and prev_day == row.day_num and prev_month == row.month:
                skip_line = False

            else:
                skip_line = True
                if len(locked_duration) != 0:
                    sleep_duration_hour = round(max(locked_duration) / 3600000)  # convert to hours
                    if sleep_duration_hour > 12:
                        sleep_duration_hour = 12

                    if sleep_duration_hour == 7:
                        sleep_score = 5
                    elif sleep_duration_hour == 6 or sleep_duration_hour == 8:
                        sleep_score = 4
                    elif sleep_duration_hour == 5 or sleep_duration_hour == 9:
                        sleep_score = 3
                    elif sleep_duration_hour == 4 or sleep_duration_hour == 10:
                        sleep_score = 2
                    else:
                        sleep_score = 1

                    sleep_duration.append(sleep_duration_hour)
                    sleep_scores.append(sleep_score)
                    users.append(row.username_id)
                    days.append(row.day_num)
                    months.append(row.month)

                    locked_duration = []  # re-init for next day or user

            if not skip_line:
                locked_duration.append(int(row.timestamp_start) - prev_time_end)

            prev_username = row.username_id
            prev_day = row.day_num
            prev_month = row.month
            prev_time_end = row.timestamp_end

    sleep_dataframe = pd.DataFrame(columns=['username_id', 'day_num', 'month', 'sleep_hours', 'sleep_score'])
    sleep_dataframe['username_id'] = users
    sleep_dataframe['day_num'] = days
    sleep_dataframe['month'] = months
    sleep_dataframe['sleep_hours'] = sleep_duration
    sleep_dataframe['sleep_score'] = sleep_scores

    sleep_dataframe.to_csv('sleep_duration.csv', index=False)


def add_sleep_values():
    df_sleep = pd.read_csv('sleep_duration.csv')
    df_main = pd.read_csv('features_output_with_social_act.csv')

    new_main_df = pd.merge(df_main, df_sleep, on=['user_id', 'day_num', 'month'])
    new_main_df.to_csv('extracted_features_with_sleep.csv', index=False)


def social_activity_value_calculation():
    weight = 10
    social_activity_threshold = 500
    dataframe = pd.read_csv('extracted_features_fixed.csv', delimiter=',', header=0)
    social_activity_values = []
    social_activity_scores = []

    for index, row in dataframe.iterrows():
        social_incoming = row['in_call_number'] * weight + row['in_call_duration']
        social_outgoing = row['out_call_number'] * weight + row['out_call_duration']
        social_apps_usage = row['app_social_communication_freq'] * weight + row['app_social_communication_dur']
        social_activity_value = social_incoming + social_outgoing + social_apps_usage
        social_activity_values.append(social_activity_value)

        if social_activity_value > social_activity_threshold * 5:
            social_activity_scores.append(5)
        elif social_activity_threshold * 4 < social_activity_value < social_activity_threshold * 5:
            social_activity_scores.append(4)
        elif social_activity_threshold * 3 < social_activity_value < social_activity_threshold * 4:
            social_activity_scores.append(3)
        elif social_activity_threshold * 2 < social_activity_value < social_activity_threshold * 3:
            social_activity_scores.append(2)
        else:
            social_activity_scores.append(1)

    dataframe['social_act_value'] = social_activity_values
    dataframe['social_act_score'] = social_activity_scores
    dataframe.to_csv('features_output_with_social_act.csv', index=False)


def convert_ema_to_symptom_scores():
    dataframe = pd.read_csv('extracted_features_sorted.csv', delimiter=',', header=0)
    for index, row in dataframe.iterrows():
        # re-init
        mood_ema = []
        food_ema = []
        sleep_ema = []
        physical_activity_ema = []
        social_activity_ema = []

        mood_ema.append(row['phq1'])
        mood_ema.append(row['phq7'])
        mood_ema.append(row['phq9'])

        food_ema.append(row['phq3'])

        sleep_ema.append(row['phq4'])

        physical_activity_ema.append(row['phq5'])
        physical_activity_ema.append(row['phq6'])

        social_activity_ema.append(row['phq2'])
        social_activity_ema.append(row['phq8'])

        mood_score = round(statistics.mean(mood_ema))
        food_score = food_ema[0]
        sleep_score = sleep_ema[0]
        physical_activity_score = round(statistics.mean(physical_activity_ema))
        social_activity_score = round(statistics.mean(social_activity_ema))

        mood_scores.append(mood_score)
        food_scores.append(food_score)
        sleep_scores.append(sleep_score)
        physical_activity_scores.append(physical_activity_score)
        social_activity_scores.append(social_activity_score)

    # adding new columns to dataframe
    dataframe['mood_gt'] = mood_scores
    dataframe['food_gt'] = food_scores
    dataframe['sleep_gt'] = sleep_scores
    dataframe['physical_act_gt'] = physical_activity_scores
    dataframe['social_act_gt'] = social_activity_scores

    dataframe.to_csv('extracted_features_with_scores(ema).csv', index=False)


def main():
    drop_no_ema_records()
    extract_features()
    fix_days_and_ema_orders()  # extracted_features_fixed.csv
    social_activity_value_calculation()  # features_output_with_social_act.csv
    get_sleep_duration()
    add_sleep_values()  # extracted features with sleep file
    sort_dataframe()  # extracted_features_sorted.csv
    convert_ema_to_symptom_scores()  # extracted_features_with_scores(ema).csv


if __name__ == "__main__":
    main()
