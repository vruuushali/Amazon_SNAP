from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from .Metadataload import metadataload
from .graphdf import getRecomm

metadf = metadataload()
# Create your views here.x
class Vroom_Vroom(APIView):
    def get(self, requests, id, group, category, top_n):
        rec= getRecomm(id, group, category, top_n)
        # rec= getRecomm('1893732290', 'Book', '|Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Christian Living[12333]|Faith[12339]', 10)
        # recommendations= getRecommendation(id='19', group=None, categories= '   |[139452]|DVD[130]|Genres[404276]|Science Fiction & Fantasy[163431]|Fantasy[163440]', top_n=5)
        json_array = rec.toJSON().collect()
        # print("JSON array:",json_array)


        return Response({
            'success':True,
            'message': 'Product Recommendation API built on networkx by Vrushali',
            'data': json_array
        })
    
    