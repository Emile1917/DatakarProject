import requests


class Req :


    def __init__(self) -> None:
        
        self.headers = requests.utils.default_headers()

        self.headers.update(
            {
                'User-Agent': 'My User Agent 1.0',
            }
        )

    def get_department(self,lat,lon):    
        url = "https://api.opencagedata.com/geocode/v1/json?q="+str(lat)+"+"+str(lon)+"&key=a1674daf25f54056a7c8047ca1742c22&no_annotations=1&language=fr"
       
        req  =requests.get(url,headers=self.headers)
        if req.status_code == 200:
            data = req.json()
            if data['results'][0]['components']['postcode']!=None:
                return data['results'][0]['components']['postcode'][:2]

        return ''