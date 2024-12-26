from jikanpy import Jikan
import pandas as pd
import jikanAnimeFetcher
import json

jikan = Jikan()

anime = jikan.anime(58059)
#print json anime and make it readable
print(json.dumps(anime, indent=4))


