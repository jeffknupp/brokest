import requests
from brokest import queue

def count_words_in_page(url):
    resp = requests.get(url)
    return len(resp.text.split())

result = queue(count_words_in_page, 'http://www.jeffknupp.com')
print result
