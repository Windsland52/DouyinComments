# config.py

# cookie
cookie = ''

# directory where logs will be saved
logs_dir = "logs"


# by creator: creator_id: str, count: int
# query type
query_type = "creator"  # Options: "creator", "detail"
# creator ids
creator_ids = [
    "MS4wLjABAAAAZJdMJWCk20BhmfjdkBtg_3OwU9tHA9aoKriwjS52wFo",  # Example: Creator ID for 哈工大
    # "......",                                                   # Add more creator IDs as needed
]
# number of videos to fetch for each creator
count = 2

# by detail: aweme_id: str
# aweme IDs to fetch details for specific videos
aweme_ids = [
    7411856833750519090,  # Example: Aweme ID for 哈工大军训又上新了
    # ......,               # Add more aweme IDs as needed
]
