import requests
import pandas as pd
from auth import *

client_id = ""
client_secret = ""

query = ("""
    query CloudConfigurationSettingsTable(
        $first: Int
        $after: String
        $filterBy: CloudConfigurationRuleFilters
        $orderBy: CloudConfigurationRuleOrder
        $projectId: [String!]
      ) {
        cloudConfigurationRules(
          first: $first
          after: $after
          filterBy: $filterBy
          orderBy: $orderBy
        ) {
          analyticsUpdatedAt
          nodes {
            id
            shortId
            name
            description
            enabled
            severity
            serviceType
            cloudProvider
            subjectEntityType
            functionAsControl
            opaPolicy
            builtin
            targetNativeTypes
            remediationInstructions
            hasAutoRemediation
            supportsNRT
            control {
              id
            }
            iacMatchers {
              id
              type
              regoCode
            }
            matcherTypes
            securitySubCategories {
              id
              title
              description
              category {
                id
                name
                description
                framework {
                  id
                  name
                  enabled
                }
              }
            }
            analytics(selection: { projectId: $projectId }) {
              passCount
              failCount
            }
            scopeAccounts {
              id
              name
              cloudProvider
            }
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
""")

# The variables sent along with the above query
variables = {
  'first': 40,
  'filterBy': {},
  'orderBy': {
    'field': 'FAILED_CHECK_COUNT',
    'direction': 'DESC'
  }
}

def query_wiz_api(query, variables):
    """Query WIZ API for the given query data schema"""
    data = {"variables": variables, "query": query}

    try:
        # Uncomment the nexvt first line and comment the line after that
        # to run behind proxies
        # result = requests.post(url="https://api.us20.app.wiz.io/graphql",
        #                        json=data, headers=HEADERS, proxies=proxyDict)
        result = requests.post(url="https://api.us20.app.wiz.io/graphql",
                               json=data, headers=HEADERS)
    except Exception as e:
        if ('502: Bad Gateway' not in str(e) and
                '503: Service Unavailable' not in str(e) and
                '504: Gateway Timeout' not in str(e)):
            print("<p>Wiz-API-Error: %s</p>" % str(e))
            return(e)
        else:
            print("Retry")
    
    return result.json()

print("Getting Token...")
request_wiz_api_token(client_id, client_secret)
print("Fetching CCRs...")
result = query_wiz_api(query, variables)

pageInfo = result['data']['cloudConfigurationRules']['pageInfo']
ccr = []
#Pagination of results and building dataframe
while (pageInfo['hasNextPage']):
    # fetch next page
    variables['after'] = pageInfo['endCursor']
    result = query_wiz_api(query, variables)
    i = 0
    #Appending ccrs 
    for x in result['data']['cloudConfigurationRules']['nodes']:
        rule = result['data']['cloudConfigurationRules']['nodes'][i]['name']
        ccr.append(rule)
        i = i + 1
    pageInfo = result['data']['cloudConfigurationRules']['pageInfo']

df = pd.DataFrame(ccr, columns=['CCR Rules'])
df.index = df.index + 1

print("Writing CSV...")
df.to_csv('ccrs.csv')
print("Complete!")