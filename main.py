import requests
import json
import os
import argparse

BASE_URL = "https://api.vk.com/method/"
API_VERSION = "5.131"
ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")


def get_user_info(user_id):
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_id,
        "fields": "followers_count",
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    response = requests.get(url, params=params).json()
    return response.get("response", [])[0] if response.get("response") else None


def get_followers(user_id):
    url = f"{BASE_URL}users.getFollowers"
    params = {
        "user_id": user_id,
        "count": 100,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    response = requests.get(url, params=params).json()
    return response.get("response", {}).get("items", [])


def get_subscriptions(user_id):
    url = f"{BASE_URL}users.getSubscriptions"
    params = {
        "user_id": user_id,
        "extended": 1,
        "count": 100,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    response = requests.get(url, params=params).json()
    #print("Response for subscriptions:", response)
    return response.get("response", {}).get("items", [])


def save_to_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_numeric_user_id(user_name):
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_name,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    response = requests.get(url, params=params).json()
    if response.get("response"):
        return response["response"][0]["id"]
    else:
        print("Error fetching user ID:", response)
        return None

def main(user_id, output_file):
    user_name = user_id
    user_id = get_numeric_user_id(user_name)
    user_info = get_user_info(user_id)
    if user_info and user_info.get("followers_count", 0) > 0:
        followers = get_followers(user_id)
        subscriptions = get_subscriptions(user_id)

        user_info.update({"followers": followers, "subscriptions": subscriptions})

        save_to_json(user_info, output_file)
        print(f"Данные сохранены в {output_file}")
    else:
        print("У пользователя нет подписчиков или подписок.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Получение информации о пользователе ВКонтакте")
    parser.add_argument("--user_id", type=str, default="olegan_west", help="ID пользователя ВКонтакте")
    parser.add_argument("--output", type=str, default="vk_data.json", help="Путь к файлу для сохранения данных")

    args = parser.parse_args()
    main(args.user_id, args.output)
