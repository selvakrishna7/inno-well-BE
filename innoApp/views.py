from django.shortcuts import render
from datetime import datetime
import pytz
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
    pipeline = [
        {
            "$group": {
                "_id": "$owner_name"
            }
        }
    ]
    results = collection.aggregate(pipeline)
    owners = [doc['_id'] for doc in results]
    print(owners)
    return JsonResponse({"unique_owners": owners})


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
        print(start_of_day.strftime("%Y-%m-%d %H:%M:%S"),".....................")
        print(now.strftime("%Y-%m-%d %H:%M:%S"),".....................")
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
    interval = request.GET.get('range')  # default: day
    if interval == 'm':
        interval = 'min'
    elif interval == 'h':
        interval = 'hr'
    elif interval == 'd':
        interval = 'month'
    elif interval == 'M':
        interval = 'month'
    elif interval == 'y':
        interval = 'year'
    if not owner_name or interval not in ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    # Map intervals to MongoDB date formats
    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": {"$in": ["volt1", "volt2", "volt3"]}
            }
        },
        {
            "$addFields": {
                "group_time": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": {"$toDate": "$time"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "time": "$group_time",
                    "volt": "$value_name"
                },
                "avg_value": {"$avg": "$value"}
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
            "$sort": {"_id": 1}
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


def get_ghg_emission(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')
    if interval == 'm':
        interval = 'min'
    elif interval == 'h':
        interval = 'hr'
    elif interval == 'd':
        interval = 'month'
    elif interval == 'M':
        interval = 'month'
    elif interval == 'y':
        interval = 'year'
    if not owner_name or interval not in  ['min', 'hr', 'day', 'month', 'year']:
        return JsonResponse({'error': 'Invalid or missing parameters'}, status=400)

    date_format_map = {
        "min": "%Y-%m-%d %H:%M",
        "hr": "%Y-%m-%d %H:00",
        "day": "%Y-%m-%d",
        "month": "%Y-%m",
        "year": "%Y"
    }

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "energy_consumption"
            }
        },
        {
            "$addFields": {
                "group_time": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": {"$toDate": "$time"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$group_time",
                "min_energy": {"$min": "$value"},
                "max_energy": {"$max": "$value"}
            }
        },
        {
            "$project": {
                "time": "$_id",
                # "energy_diff": {"$subtract": ["$max_energy", "$min_energy"]},
                "ghg_emission": {
                    "$round": [{"$multiply": [{"$subtract": ["$max_energy", "$min_energy"]}, 0.00058]}, 6]
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



def get_energy_consumption(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    # Map shorthand interval to full
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

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "energy_consumption"
            }
        },
        {
            "$addFields": {
                "group_time": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": {"$toDate": "$time"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$group_time",
                "min_energy": {"$min": "$value"},
                "max_energy": {"$max": "$value"}
            }
        },
        {
            "$project": {
                "time": "$_id",
                "energy_diff": {
                    "$subtract": ["$max_energy", "$min_energy"]
                },
                "ghg_emission": {
                    "$round": [
                        {"$multiply": [
                            {"$subtract": ["$max_energy", "$min_energy"]},
                        1
                        ]},
                        6
                    ]
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

from django.http import JsonResponse

def get_power_factor(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    # Interval mapping
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

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "powerfactor"
            }
        },
        {
            "$addFields": {
                "group_time": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": {"$toDate": "$time"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$group_time",
                "avg_power_factor": {"$avg": "$value"}
            }
        },
        {
            "$project": {
                "time": "$_id",
                "power_factor": {
                    "$round": ["$avg_power_factor", 3]
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

def get_current_stats(request):
    owner_name = request.GET.get('owner')
    interval = request.GET.get('range')

    # Interval mapping
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

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": {"$in": ["current1", "current2", "current3"]}
            }
        },
        {
            "$addFields": {
                "group_time": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": {"$toDate": "$time"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "time": "$group_time",
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

    # Interval mapping
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

    pipeline = [
        {
            "$match": {
                "owner_name": owner_name,
                "value_name": "frequency"
            }
        },
        {
            "$addFields": {
                "group_time": {
                    "$dateToString": {
                        "format": date_format_map[interval],
                        "date": {"$toDate": "$time"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$group_time",
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
