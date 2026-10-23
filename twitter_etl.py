
import os
import csv
import tweepy


def access_api():

    # Read the Bearer Token from the environment.
    #bearer_token = os.getenv("X_BEARER_TOKEN")

    #if not bearer_token:
     #   raise ValueError(
     #       "Missing X_BEARER_TOKEN environment variable. "
     #       "Set it before running the script."
     #   )

    # Initialize the X API v2 client.
    client = tweepy.Client(
        bearer_token="AAAAAAAAAAAAAAAAAAAAAHHr%2FwEAAAAAvSKWPMB6BROPmJRLbb6b7JgxKlo%3D5zXq90Wb317TNBLc04E1pFgN7pArL7BbOvZmlsACDXMNLP3iZA",
        wait_on_rate_limit=True,
    )

    try:
        # Get the user ID.
        user_response = client.get_user(
            username="elonmusk"
        )

        if not user_response.data:
            print("User not found or inaccessible.")
            return

        user = user_response.data
        print(f"Fetching tweets for @{user.username}")

        # Fetch recent tweets, excluding retweets and replies.
        response = client.get_users_tweets(
            id=user.id,
            max_results=100,
            exclude=["retweets", "replies"],
            tweet_fields=["created_at", "public_metrics"],
        )

        tweets = response.data or []

        if not tweets:
            print("No tweets returned. Check your API access.")
            return

        # Print tweets.
        for tweet in tweets:
            print(f"ID: {tweet.id}")
            print(f"Date: {tweet.created_at}")
            print(f"Text: {tweet.text}")
            print("-" * 60)

        # Save tweets to CSV.
        with open(
            "elonmusk_tweets.csv",
            "w",
            newline="",
            encoding="utf-8",
        ) as file:
            fieldnames = [
                "id",
                "created_at",
                "text",
                "like_count",
                "retweet_count",
            ]

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
            )
            writer.writeheader()

            for tweet in tweets:
                metrics = tweet.public_metrics or {}

                writer.writerow({
                    "id": tweet.id,
                    "created_at": tweet.created_at,
                    "text": tweet.text,
                    "like_count": metrics.get("like_count", 0),
                    "retweet_count": metrics.get("retweet_count", 0),
                })

        print(
            f"Saved {len(tweets)} tweets to "
            "elonmusk_tweets.csv"
        )

    except tweepy.TweepyException as error:
        print(f"X API request failed: {error}")


if __name__ == "__main__":
    twitter_tweets()
