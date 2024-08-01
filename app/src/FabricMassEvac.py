# In[38]:
import requests
import base64
import time
from time import gmtime, strftime
import jmespath
import pandas as pd
import os, os.path
import shutil

# In[39]:
def Extract(workspaceIds = None):
    #workspaceIds = [] #["8aee58cf-be0d-4548-a78d-fe04f271487c","c39f260a-4e38-40de-aac0-e25e5dd621eb"]
    if workspaceIds is None: 
        workspaceIds = []
    else:
        assert type(workspaceIds) == list, "Input variables should be list of workspace GUIDs"

    absPath = os.path.join(mssparkutils.nbResPath,'builtin')
    tempDir = os.path.join(absPath,'temp')
    fileName = os.path.join(absPath, 'FabricExport '+strftime("%Y-%m-%d_%H-%M-%S", gmtime()))

    token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    header = {'Content-Type':'application/json','Authorization': f'Bearer {token}'}

    workspaces = []
    # get list of all acessible workspaces if none specificed
    if len(workspaceIds) < 1:
        response = requests.get(url=f'https://api.fabric.microsoft.com/v1/workspaces', headers=header)
        match response.status_code:
            case 200:
                workspaces += jmespath.search('value[*].{id:id,name:displayName}', response.json())
            case _:
                print(response.status_code)
                print(response.headers)
    else:
        for ws in workspaceIds:
            response = requests.get(url=f'https://api.fabric.microsoft.com/v1/workspaces/{ws}', headers=header)
            match response.status_code:
                case 200:
                    j = response.json()
                    workspaces.append( {'id': j.get('id'), 'name': j.get('displayName')})
                case _:
                    print(response.status_code)
                    print(response.headers)

    print(workspaces)

    # Get artefacts/items for each workspace
    itemList = []
    for ws in workspaces:
        itemList += GetItems(ws['id'],header)

    # Filter artefacts for notebooks, and then retrieve
    payloadList = []
    GetDefinitions( jmespath.search('[?type == `Notebook`]', itemList), payloadList, 'ipynb', header)

    # determine workspace name for each item+payload by referencing workspaces dict list
    payloads = pd.merge(pd.DataFrame(payloadList), pd.DataFrame(workspaces), left_on='workspaceId', right_on='id', how='left')

    for index, row in payloads.iterrows():
        WriteFile( os.path.join( tempDir, row['name_y']), row['name_x']+'.ipynb', row['payload'][0])

    # Create zip file
    fileName = os.path.join(absPath, 'FabricExport '+strftime("%Y-%m-%d_%H-%M-%S", gmtime()))  
    shutil.make_archive( fileName, 'zip', tempDir)
    shutil.rmtree( tempDir)


# In[40]:


def GetItems(workspaceId, header):
    response = requests.get(url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items', headers=header)
    if response.status_code == 200:
            return jmespath.search('value[*]', response.json())
    else:
        print(response.status_code)
        print(response.headers)
        return None


# In[41]:


def GetDefinitions(items, payloadList, format, header):
    formatParam = ''
    if format is not None: 
        formatParam = 'format='+format
    
    LROList = [] # used to hold list of items where payload is generated via long running operation, and need to check status of job
    print('Items '+str(len(items)))
    for item in items:
        workspaceId = item.get('workspaceId')
        itemId = item.get('id')
        name = item.get('displayName')
        
        response = requests.post(url=f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition?'+formatParam, headers=header)
        match response.status_code:
            case 200:
                # need to add logic to add each part as separate payload - can be a problem in other artefacts
                payloadList.append( {'id': itemId, 'name': name, 'workspaceId': workspaceId, 'payload': jmespath.search('definition.parts[*].payload', response.json())})
            case 202:
                LROList.append( {'id': itemId, 'name': name, 'workspaceId': workspaceId, 'location': response.headers.get('Location')})  #int(notNone(response.headers.get('Retry-After'), 30))
            case _:
                print(response.status_code)
                print(response.headers)
                print(response.text)

    print('Payloads '+str(len(payloadList))) #print(payloadList)
    print('LROs '+str(len(LROList))) #print(LROList)
    # if GetDefintion returned async long running operations (LRO) to create files
    # wait+iterate until all items fetched - this needs some extra error handling
    while len(LROList) > 0:
        time.sleep(15) # wait and retry all LROs
        for i, LRO in enumerate(LROList):
            response_LRO = requests.get(url= LRO['location'] , headers=header)

            if response_LRO.status_code == 200 and response_LRO.json().get('status') == 'Succeeded':
                response_result = requests.get(url=LRO['location']+"/result", headers=header)
                payloadList.append( {'id': LRO['id'], 'name': LRO['name'], 'workspaceId': LRO['workspaceId'], 'payload': jmespath.search('definition.parts[*].payload', response_result.json())})
                LROList[i] = None # Remove LRO operation from list
            else:
                print(response.status_code)
                print(response.headers)
                print(response.text)
                LROList[i] = None # Remove LRO operation from list - error occurred
        LROList = [n for n in LROList if n is not None]
        print("Next LRO: "+str(len(LROList)))


# In[42]:


def WriteFile(location, name, base64payload):
    decoded = base64.b64decode( base64payload)
    #mssparkutils.fs.put({mssparkutils.nbResPath}"/env/Output.ipynb", decoded.decode("utf-8"), True) # Set the last parameter as True to overwrite the file if it existed already
    path = location+'/'+name
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding="utf-8") as output_file:
        output_file.write(decoded.decode("utf-8"))
