# Collector


## API endpoints

### every 15 minutes

- /friends/ids

	- gets user friends

		- in chronological order (latest first)

	- 15 calls

	- groups of 5,000 user IDs

		- ergo max. 75000 friends in 15 minutes

	- pagination

- /users/lookup

	- gets user details (such as number of friends, timezone, and language)

	- 900 calls

	- max 100 users

		- ergo max. 90 000 users in 15 minutes

- /friendships/show

	- get information about relationship between two users

	- 180 calls

## process

### use a token until it's empty, then take next

- needs more than one collector/connection to take different limits into account

- adjust number of processes dynamically

	- call new process/thread if not quick enough to use 15 minutes as efficiently as necessary

		- would need common stack(s) to take tokens from

	- close process/thread if more than one process has nothing to collect anymore, but time remaining

		- needs supervising process/class/object

### 1. define number of seeds n

- most likely number of tokens times 15

### 2. choose n random seeds from seed set

### 3. store as queue

### 4. work through seeds to

- get all friends with details

	- look in cached data

	- if not found there request API

		- store number of followers, timezone and interface language

- find most followed friend of each seed, who has German as interface language and correct timezone.

	- if edge is not in sample

		- becomes new seed

	- while edge is already in sample and burned

		- go for next most followed friend

		- if edge is not burned or not in sample

			- becomes new seed

	- if no new seed can be found or already in new seeds, draw random seed from seed set

- find out whether relationship is reciprocal

	- store relationships in new edge list (make sure not to create duplicates)

		- mark outgoing edge as burned

### 5. start again at 3 with new sseds

