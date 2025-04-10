from django.shortcuts import render
from datetime import datetime
import pytz
from datetime import datetime, timedelta
# yourapp/views.py
from django.http import HttpResponse
from django.http import JsonResponse
from pymongo import MongoClient
# from .mqtt_client import client
client = MongoClient("mongodb://localhost:27017/")
db = client["iot_data"]
collection = db["em_data"]
def publish_message(request):
    client.publish("test/topic", "Hello from Django!")
    return HttpResponse("Message sent.")






def get_unique_owners(request):
    # Use strings if owner_name is stored as string
    custom_order = ['11', '12', '16', '17', '18', '13', '10', '7', '8', '9']

    pipeline = [
        {
            "$group": {
                "_id": "$owner_name"
            }
        }
    ]
    results = collection.aggregate(pipeline)
    owners = [str(doc['_id']) for doc in results]  # make sure all IDs are strings

    # Sort by custom order
    ordered_owners = sorted(owners, key=lambda x: custom_order.index(x) if x in custom_order else float('inf'))

    return JsonResponse({"unique_owners": ordered_owners})


def compute_energy_consumption(owner_name, start_time, end_time):
    
    pipeline = [
        {
            '$match': {
                'owner_name': owner_name,
                'value_name': 'energy_consumption',
                'time': {
                    '$gte': start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    '$lte': end_time.strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        },
        {
            '$sort': {'time': 1}
        },
        {
            '$project': {
                '_id': 0,
                'value': 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    if len(results) >= 2:
        start_val = results[0]['value']
        end_val = results[-1]['value']
        return round(end_val - start_val, 2)
    return 0

def get_energy_stats(request):
    owner_name = request.GET.get('owner')
    if not owner_name:
        return JsonResponse({'error': 'Missing owner_name'}, status=400)

    india_timezone = pytz.timezone('Asia/Kolkata')
    now = datetime.now(india_timezone)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_of_year = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    unit_cost = 8.25

    try:
        one_day_energy = compute_energy_consumption(owner_name, start_of_day, now)
        one_month_energy = compute_energy_consumption(owner_name, start_of_month, now)
        one_year_energy = compute_energy_consumption(owner_name, start_of_year, now)

        return JsonResponse({
            "owner_name": owner_name,
            "one_day_energy": round(one_day_energy, 2),
            "one_day_cost": round(one_day_energy * unit_cost, 2),
            "one_month_energy": round(one_month_energy, 2),
            "one_month_cost": round(one_month_energy * unit_cost, 2),
            "one_year_energy": round(one_year_energy, 2),
            "one_year_cost": round(one_year_energy * unit_cost, 2)
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

def get_voltage_stats(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    interval_map = {
        'm': 'min',
        'h': 'hr',
        'd': 'day',
        'M': 'month',
        'y': 'year'
    }
    interval = interval_map.get(interval, interval)

    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    now = datetime.now(IST)
    if interval == 'min':
        from_time = now - timedelta(minutes=60)
    elif interval == 'hr':
        from_time = now - timedelta(hours=24)
    elif interval == 'day':
        from_time = now - timedelta(days=30)
    elif interval == 'month':
        from_time = now.replace(day=1) - timedelta(days=365)
    elif interval == 'year':
        from_time = now.replace(month=1, day=1) - timedelta(days=365 * 5)

    from_time_utc = from_time.astimezone(IST)

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": { "$in": ["volt1", "volt2", "volt3"] }
            }
        },
        {
            "$addFields": {
                "parsed_time": { "$toDate": "$time" }
            }
        },
        {
            "$match": {
                "parsed_time": { "$gte": from_time_utc }
            }
        },
        {
            "$group": {
                "_id": {
                    "time": {
                        "$dateToString": {
                            "format": date_format_map[interval],
                            "date": "$parsed_time",
                            "timezone": "Asia/Kolkata"
                        }
                    },
                    "volt": "$value_name"
                },
                "avg_value": { "$avg": "$value" }
            }
        },
        {
            "$group": {
                "_id": "$_id.time",
                "voltages": {
                    "$push": {
                        "volt": "$_id.volt",
                        "value": "$avg_value"
                    }
                }
            }
        },
        {
            "$sort": { "_id": 1 }
        }
    ]

    results = list(collection.aggregate(pipeline))

    output = []
    for entry in results:
        volt_data = {
            "time": entry["_id"],
            "volt1": None,
            "volt2": None,
            "volt3": None
        }
        for v in entry["voltages"]:
            volt_data[v["volt"]] = round(v["value"], 2)
        output.append(volt_data)

    return JsonResponse({
        "owner_name": owner_name,
        "interval": interval,
        "data": output
    })



from datetime import datetime, timedelta
from django.http import JsonResponse
from dateutil import parser  # safer parsing
from pymongo import ASCENDING
from datetime import datetime, timedelta

IST = pytz.timezone('Asia/Kolkata')
from django.http import JsonResponse
from datetime import datetime, timedelta
from pymongo import ASCENDING
from pytz import timezone

IST = timezone("Asia/Kolkata")

def get_ghg_emission(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    interval_map = {
        'm': 'min',
        'h': 'hr',
        'd': 'day',
        'M': 'month',
        'y': 'year'
    }
    interval = interval_map.get(interval, interval)

    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    now = datetime.now(IST)

    if interval == 'min':
        from_time = now.replace(second=0, microsecond=0) - timedelta(minutes=59)
        to_time = now
        date_format = "%Y-%m-%d %H:%M"
        time_slots = [(from_time + timedelta(minutes=i)).strftime(date_format) for i in range(60)]

    elif interval == 'hr':
        from_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=23)
        to_time = now
        date_format = "%Y-%m-%d %H:00"
        time_slots = [(from_time + timedelta(hours=i)).strftime(date_format) for i in range(24)]

    elif interval == 'day':
        from_time = now.replace(hour=0, minute=0, second=0, microsecond=0).replace(day=1)
        to_time = now
        total_days = (now - from_time).days + 1
        date_format = "%Y-%m-%d"
        time_slots = [(from_time + timedelta(days=i)).strftime(date_format) for i in range(total_days)]

    elif interval == 'month':
        from_time = now.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0)
        to_time = now
        date_format = "%Y-%m"
        time_slots = []
        for i in range(1, now.month + 1):
            time_slots.append(datetime(now.year, i, 1).strftime(date_format))

    elif interval == 'year':
        first_doc = collection.find_one({
            "owner_name": owner_name,
            "value_name": "energy_consumption"
        }, sort=[("time", ASCENDING)])

        if not first_doc:
            return JsonResponse({
                "owner_name": owner_name,
                "interval": interval,
                "data": []
            })

        earliest_time = datetime.strptime(first_doc["time"], "%Y-%m-%d %H:%M:%S")
        from_time = earliest_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        to_time = now
        date_format = "%Y"
        time_slots = [str(y) for y in range(from_time.year, now.year + 1)]

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "energy_consumption",
                "time": {
                    "$gte": from_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "$lte": to_time.strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        },
        {
            "$addFields": {
                "parsed_time": { "$toDate": "$time" }
            }
        },
        {
            "$project": {
                "value": 1,
                "bucket": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$parsed_time",
                        "timezone": "Asia/Kolkata"
                    }
                },
                "parsed_time": 1
            }
        },
        {
            "$sort": { "parsed_time": 1 }
        },
        {
            "$group": {
                "_id": "$bucket",
                "first_energy": { "$first": "$value" },
                "last_energy": { "$last": "$value" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "ghg_emission": {
                    "$round": [{ "$multiply": [
                        { "$max": [{ "$subtract": ["$last_energy", "$first_energy"] }, 0] },
                        0.00058
                    ]}, 6]
                }
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    # Make sure all expected time slots are included (even with 0 emission)
    ghg_dict = {r["time"]: r["ghg_emission"] for r in results}
    final_result = [{"time": slot, "ghg_emission": ghg_dict.get(slot, 0)} for slot in time_slots]

    return JsonResponse({
        "owner_name": owner_name,
        "interval": interval,
        "data": final_result
    })



import pytz
IST = pytz.timezone('Asia/Kolkata')
def get_energy_consumption(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    interval_map = {
        'm': 'min',
        'h': 'hr',
        'd': 'day',
        'M': 'month',
        'y': 'year'
    }
    interval = interval_map.get(interval, interval)

    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    now = datetime.now(IST)

    if interval == 'min':
        start = now.replace(second=0, microsecond=0, minute=0)
        end = start + timedelta(hours=1)
    elif interval == 'hr':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif interval == 'day':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end = start.replace(year=now.year + 1, month=1)
        else:
            end = start.replace(month=now.month + 1)
    elif interval == 'month':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)
    elif interval == 'year':
        match_stage = {
            "$match": {
                "owner_name": owner_name,
                "value_name": "energy_consumption"
            }
        }
        time_add = {"$addFields": {"parsed_time": {"$toDate": "$time"}}}
        sort_stage = {"$sort": {"parsed_time": 1}}
        limit_stage = {"$limit": 1}
        earliest = list(collection.aggregate([match_stage, time_add, sort_stage, limit_stage]))
        if not earliest:
            return JsonResponse({"owner_name": owner_name, "interval": interval, "data": []})
        start = earliest[0]['parsed_time']
        end = now

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "energy_consumption"
            }
        },
        {
            "$addFields": {
                "parsed_time": {"$toDate": "$time"}
            }
        },
        {
            "$match": {
                "parsed_time": {"$gte": start, "$lt": end}
            }
        },
        {
            "$sort": {"parsed_time": 1}
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": "$parsed_time",
                        "timezone": "Asia/Kolkata"
                    }
                },
                "first": {"$first": "$value"},
                "last": {"$last": "$value"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "energy": {"$round": [{"$subtract": ["$last", "$first"]}, 6]}
            }
        },
        {
            "$sort": {"time": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    full_range = []
    if interval == 'min':
        for i in range(60):
            t = (start + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M")
            full_range.append(t)
    elif interval == 'hr':
        for i in range(24):
            t = (start + timedelta(hours=i)).strftime("%Y-%m-%d %H:00")
            full_range.append(t)
    elif interval == 'day':
        day_count = (end - start).days
        for i in range(day_count):
            t = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            full_range.append(t)
    elif interval == 'month':
        year = now.year
        for i in range(1, 13):
            full_range.append(f"{year}-{i:02}")
    elif interval == 'year':
        year_start = start.year
        year_end = now.year
        for y in range(year_start, year_end + 1):
            full_range.append(str(y))

    final_data = []
    result_dict = {item['time']: item['energy'] for item in results}

    for t in full_range:
        final_data.append({
            "time": t,
            "energy": result_dict.get(t, 0)
        })

    return JsonResponse({
        "owner_name": owner_name,
        "interval": interval,
        "data": final_data
    })





from django.http import JsonResponse
def get_power_factor(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    interval_map = {
        'm': 'min',
        'h': 'hr',
        'd': 'day',
        'M': 'month',
        'y': 'year'
    }
    interval = interval_map.get(interval, interval)

    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    now = datetime.now(IST)

    if interval == 'min':
        start = now.replace(second=0, microsecond=0, minute=0)
        end = start + timedelta(hours=1)
    elif interval == 'hr': 
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif interval == 'day':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end = start.replace(year=now.year + 1, month=1)
        else:
            end = start.replace(month=now.month + 1)
    elif interval == 'month':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)
    elif interval == 'year':
        match_stage = {
            "$match": {
                "owner_name": owner_name,
                "value_name": "powerfactor"
            }
        }
        time_add = {"$addFields": {"parsed_time": {"$toDate": "$time"}}}
        sort_stage = {"$sort": {"parsed_time": 1}}
        limit_stage = {"$limit": 1}
        earliest = list(collection.aggregate([match_stage, time_add, sort_stage, limit_stage]))
        if not earliest:
            return JsonResponse({"owner_name": owner_name, "interval": interval, "data": []})
        start = earliest[0]['parsed_time']
        end = now

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "powerfactor"
            }
        },
        {
            "$addFields": {
                "parsed_time": {"$toDate": "$time"}
            }
        },
        {
            "$match": {
                "parsed_time": {"$gte": start, "$lt": end}
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": "$parsed_time",
                        "timezone": "Asia/Kolkata"
                    }
                },
                "avg_power_factor": {"$avg": "$value"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "power_factor": {"$round": ["$avg_power_factor", 3]}
            }
        },
        {
            "$sort": {"time": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    return JsonResponse({
        "owner_name": owner_name,
        "interval": interval,
        "data": results
    })

def get_current_stats(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    interval_map = {
        'm': 'min',
        'h': 'hr',
        'd': 'day',
        'M': 'month',
        'y': 'year'
    }
    interval = interval_map.get(interval, interval)

    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    now = datetime.now(IST)

    if interval == 'min':
        start = now.replace(second=0, microsecond=0, minute=0)
        end = start + timedelta(hours=1)
    elif interval == 'hr':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif interval == 'day':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end = start.replace(year=now.year + 1, month=1)
        else:
            end = start.replace(month=now.month + 1)
    elif interval == 'month':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)
    elif interval == 'year':
        match_stage = {
            "$match": {
                "owner_name": owner_name,
                "value_name": {"$in": ["current1", "current2", "current3"]}
            }
        }
        time_add = {"$addFields": {"parsed_time": {"$toDate": "$time"}}}
        sort_stage = {"$sort": {"parsed_time": 1}}
        limit_stage = {"$limit": 1}
        earliest = list(collection.aggregate([match_stage, time_add, sort_stage, limit_stage]))
        if not earliest:
            return JsonResponse({"owner_name": owner_name, "interval": interval, "data": []})
        start = earliest[0]['parsed_time']
        end = now

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": {"$in": ["current1", "current2", "current3"]}
            }
        },
        {
            "$addFields": {
                "parsed_time": {"$toDate": "$time"}
            }
        },
        {
            "$match": {
                "parsed_time": {"$gte": start, "$lt": end}
            }
        },
        {
            "$group": {
                "_id": {
                    "time": {
                        "$dateToString": {
                            "format": date_format_map[interval],
                            "date": "$parsed_time",
                            "timezone": "Asia/Kolkata"
                        }
                    },
                    "phase": "$value_name"
                },
                "avg_value": {"$avg": "$value"}
            }
        },
        {
            "$project": {
                "time": "$_id.time",
                "phase": "$_id.phase",
                "value": {"$round": ["$avg_value", 2]}
            }
        },
        {
            "$group": {
                "_id": "$time",
                "data": {
                    "$push": {
                        "k": "$phase",
                        "v": "$value"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "a1": {
                    "$let": {
                        "vars": {"map": {"$arrayToObject": "$data"}},
                        "in": {"$ifNull": ["$$map.current1", None]}
                    }
                },
                "a2": {
                    "$let": {
                        "vars": {"map": {"$arrayToObject": "$data"}},
                        "in": {"$ifNull": ["$$map.current2", None]}
                    }
                },
                "a3": {
                    "$let": {
                        "vars": {"map": {"$arrayToObject": "$data"}},
                        "in": {"$ifNull": ["$$map.current3", None]}
                    }
                }
            }
        },
        {
            "$sort": {"time": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    return JsonResponse({
        "owner_name": owner_name,
        "interval": interval,
        "data": results
    })


def get_frequency_stats(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    interval_map = {
        'm': 'min',
        'h': 'hr',
        'd': 'day',
        'M': 'month',
        'y': 'year'
    }
    interval = interval_map.get(interval, interval)

    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    now = datetime.now(IST)

    if interval == 'min':
        start = now.replace(second=0, microsecond=0, minute=0)
        end = start + timedelta(hours=1)
    elif interval == 'hr':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif interval == 'day':
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end = start.replace(year=now.year + 1, month=1)
        else:
            end = start.replace(month=now.month + 1)
    elif interval == 'month':
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)
    elif interval == 'year':
        match_stage = {
            "$match": {
                "owner_name": owner_name,
                "value_name": "frequency"
            }
        }
        time_add = {"$addFields": {"parsed_time": {"$toDate": "$time"}}}
        sort_stage = {"$sort": {"parsed_time": 1}}
        limit_stage = {"$limit": 1}
        earliest = list(collection.aggregate([match_stage, time_add, sort_stage, limit_stage]))
        if not earliest:
            return JsonResponse({"owner_name": owner_name, "interval": interval, "data": []})
        start = earliest[0]['parsed_time']
        end = now

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "frequency"
            }
        },
        {
            "$addFields": {
                "parsed_time": {"$toDate": "$time"}
            }
        },
        {
            "$match": {
                "parsed_time": {"$gte": start, "$lt": end}
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": "$parsed_time",
                        "timezone": "Asia/Kolkata"
                    }
                },
                "avg_frequency": {"$avg": "$value"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "time": "$_id",
                "frequency": {"$round": ["$avg_frequency", 2]}
            }
        },
        {
            "$sort": {"time": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    return JsonResponse({
        "owner_name": owner_name,
        "interval": interval,
        "data": results
    })
