#!/usr/bin/env python
# coding: utf-8

# In[54]:


import elasticsearch
from elasticsearch import Elasticsearch
from faker import Faker
fake = Faker()


# In[55]:


from elasticsearch import helpers


# In[13]:


#es = Elasticsearch("https://localhost:9200")


# In[56]:


ELASTIC_PASSWORD = "rjCJpK2VB4UHVu*NvoyA"
# Create the client instance
client = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/home/adduser/elasticsearch-8.2.0/config/certs/http_ca.crt",
    basic_auth=("elastic", ELASTIC_PASSWORD)
)

# Successful response!
client.info()


# In[16]:


doc = {"name":fake.name(), "street": fake.street_address(), "city": fake.city(), "zip": fake.zipcode() }
#inserting single document


# In[17]:


res = client.index(index="users", document=doc)
print(res['result'])


# In[18]:


actions =[{
    "_index": "users",
#     "_type": "document",
    "_source":{
        "name": fake.name(),
        "street": fake.street_address(),
        "city": fake.city(),
        "zip": fake.zipcode()}
   }
for n in range(998)]


# In[19]:


res = helpers.bulk(client, actions) #helpers library use to insert bulk or many document at once
print(res[0])


# In[20]:


doc = {"query": {"match_all":{}}}


# In[24]:


res = client.search(body={"query": {"match_all":{}}}, index="users",size=10)


# In[25]:


print(res)


# In[26]:


print(res['hits']['hits'])


# In[28]:


for doc in res['hits']['hits']:
    print(doc['_source'])


# In[33]:


from pandas import json_normalize as pj


# In[34]:


df = pj(res['hits']['hits'])  #load json into a pandas dataframe


# In[35]:


df.head()


# In[40]:


doc={"query":{"match":{"name":"Ronald Goodman"}}}
res=client.search(index="users",body=doc,size=10)


# In[41]:


print(res['hits']['hits'][0]['_source'])


# In[43]:


res=client.search(index="users",q="name:Ronald Goodman",size=10) #the q is known as using the Lucene syntax where you specify the field:value


# In[44]:


print(res['hits']['hits'][0]['_source'])


# In[46]:


# Get City Jamesberg - Returns Jamesberg and Lake Jamesberg, but this dataset is not having two jamesberg
doc={"query":{"match":{"city":"Jamesberg"}}}
res=client.search(index="users",body=doc,size=10)


# In[47]:


print(res['hits']['hits'])


# In[51]:


# Get Jamesberg and filter on zip so Lake Jamesberg is removed
doc={"query":{"bool":{"must":{"match":{"city":"Jamesberg"}},
"filter":{"term":{"zip":"63792"}}}}}
res=client.search(index="users",body=doc,size=10)


# In[52]:


print(res['hits']['hits'])


# #### Comments comes after code execution. Take Note

# In[58]:


res = client.search(
index = 'users',
# doc_type = 'doc',
scroll = '20m',
size = 500,
body = {"query":{"match_all":{}}}
)  #using scroll to access more than 10,000 records. though we dont have up to 10,000 records. for teaching purposes


# #### Search your data. Since you do not have over 10,000 records, you will set the size to
# 500. This means you will be missing 500 records from your initial search. You will
# pass a new parameter to the search method – scroll. This parameter specifies
# how long you want to make the results available for. I am using 20 milliseconds.
# Adjust this number to make sure you have enough time to get the data – it will
# depend on the document size and network speed:

# In[59]:


sid = res['_scroll_id']
size = res['hits']['total']['value']


# #### 
# The results will include _scroll_id, which you will need to pass to the scroll
# method later. Save the scroll ID and the size of the result set:

# In[1]:


while (size > 0):
    res = client.scroll(scroll_id = sid, scroll = '20m')


# #### To start scrolling, use a while loop to get records until the size is 0, meaning
# #### there is no more data. Inside the loop, you will call the scroll method and pass
# #### _scroll_id and how long to scroll. This will grab more of the results from the
# #### original query:

# In[ ]:


sid = res['_scroll_id']
size = len(res['hits']['hits'])


# ##### Next, get the new scroll ID and the size so that you can loop through again if the data still exists:

# In[ ]:


for doc in res['hits']['hits']:
print(doc['_source'])


# ##### Lastly, you can do something with the results of the scrolls. In the following code, you will print the source for every record:

# In[ ]:




