#!/usr/bin/env python

'''
Returns Land/Water percentages for input geojson polygons. 
'''

from __future__ import print_function
from __future__ import division
from builtins import range
from builtins import object
import os
import json
import pyproj
from functools import partial
from shapely.geometry import shape, Polygon, MultiPolygon, mapping
from shapely.ops import cascaded_union
from shapely.validation import explain_validity
import shapely.ops
from shapely import speedups
import fiona

speedups.enable()
land_shapes = False #globals for speed
water_shapes = False #globals for speed

def covers_land(geojson):
    '''Determines if there is any land over the geojson. Returns True or False'''
    geojson = validate_geojson(geojson)
    global land_shapes
    if not land_shapes:
        land_shapes = get_shapes(oftype='land')
    for shapeobj in land_shapes:
        if shapeobj.intersects(geojson):
            return True
    return False

def covers_water(geojson):
    '''Determines if there is any water over the geojson. Returns True or False'''
    geojson = validate_geojson(geojson)
    global water_shapes
    if not water_shapes:
        water_shapes = get_shapes(oftype='water')
    for shapeobj in water_shapes:
        if shapeobj.intersects(geojson):
            return True
    return False

def covers_only_land(geojson):
    '''returns True if the geojson only covers land, False if there is any water in the scene'''
    return not covers_water(geojson)

def covers_only_water(geojson):
    '''returns True if the geojson only covers water, False if there is any land in the scene'''
    return not covers_land(geojson)

def get_land_area(geojson):
    '''returns the amount of land covering the geojson in km^2'''
    geojson = validate_geojson(geojson)
    global land_shapes
    if not land_shapes:
        land_shapes = get_shapes(oftype='land')
    intersecting_land_shapes = []
    for shapeobj in land_shapes:
        if shapeobj.intersects(geojson) or geojson.contains(shapeobj):
            if shapeobj.contains(geojson):
                return get_area(geojson)
            intersecting_land_shapes.append(geojson.intersection(shapeobj))
    area = get_area(MultiPolygon(intersecting_land_shapes))
    return area

def get_water_area(geojson):
    '''Returns the amount of water covering the geojson in km^2'''
    geojson = validate_geojson(geojson)
    land = get_land_area(geojson)
    return get_area(shape(geojson)) - land

def get_land_percentage(geojson):
    '''Returns the percentage of area covered by land. 0.0 to 1.0'''
    geojson = validate_geojson(geojson)
    return get_land_area(geojson) / get_area(geojson)

def get_water_percentage(geojson):
    '''Returns the percentage of area covered by water. 0.0 to 1.0'''
    geojson = validate_geojson(geojson)
    return get_water_area(geojson) / get_area(geojson)

def get_polygons(geojson, oftype='land'):
    '''returns a list of land area polygons that intersect the input geojson, for either land or water, cropped to the input geojson extent'''
    intersecting_shapes = []
    geojson = validate_geojson(geojson)
    landwater_shapes = get_shapes(oftype=oftype)
    for shapeobj in landwater_shapes:
        if shapeobj.intersects(geojson):
            if shapeobj.contains(geojson):
                return mapping(geojson)
            intrsct = geojson.intersection(shapeobj)
            intersecting_shapes.append(intrsct)
    if len(intersecting_shapes) == 0:
        return None
    if len(intersecting_shapes) == 1:
        return mapping(intersecting_shapes[0])
    multi = MultiPolygon(intersecting_shapes)
    return mapping(cascaded_union(multi))

def get_land_polygons(geojson):
    '''returns a list of land area polygons that intersect the input geojson, cropped to the input geojson extent'''
    return get_polygons(geojson, oftype='land')

def get_water_polygons(geojson):
    '''returns a list of water area polygons that intersect the input geojson, cropped to the input geojson extent'''
    return get_polygons(geojson, oftype='water')

def get_area(geojson):
    '''Returns the area of the polygon'''
    geojson = validate_geojson(geojson)
    newshape = shapely.ops.transform(partial(pyproj.transform, pyproj.Proj(init='EPSG:4326'),
                                     pyproj.Proj(proj='aea')), geojson)
    return newshape.area / 10.0**6

def get_shapes(oftype='land'):
    '''loads all the shapes from the water shapefile'''
    shapefile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'simplified_{0}_polygons.shp'.format(oftype))
    shapes = []
    if not os.path.exists(shapefile):
        raise Exception('Required data file does not exist: {0}'.format(shapefile))
    with fiona.collection(shapefile, 'r') as inp:
        for geom in inp:
            sp = validate_geojson(geom['geometry'])#shape(geom['geometry'])
            shapes.append(sp)
    return shapes


def validate_geojson(geojson):
    '''validates the geojson and converts it into a shapely object. can accept strings, shapefiles & geojson dicts'''
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    if isinstance(geojson, shapely.geometry.polygon.Polygon):
        return geojson
    if isinstance(geojson, shapely.geometry.multipolygon.MultiPolygon):
        return geojson
    shp = shape(geojson)
    if shp.is_valid:
        return shp
    else:
        shp = shp.buffer(0)# handle self-intersection
        if shp.is_valid:
            return shp
        else:
            print(type(geojson))
            raise Exception('input geojson is not valid: {}'.format(explain_validity(shp)))

class bcolors(object):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def comparison(input_val, comparison_val):
    '''Returns PASSED or FAILED strings if values are comparable.'''
    failed = bcolors.FAIL + 'FAILED' + bcolors.ENDC
    passed = bcolors.OKBLUE + 'PASSED' + bcolors.ENDC
    if type(input_val) is bool:
        if input_val == comparison_val:
            return passed
        return failed
    if input_val == 0: #handle zeros
        if comparison_val < 0.1:
            return passed
        return failed
    if (input_val - comparison_val) / input_val > 0.1:
        print(input_val, comparison_val)
        return failed
    return passed
         
def test():
    '''runs a test over sicily, hawaii, etc'''
    test_dict = {}
    result = {}
    test_dict['land_and_water_polygon'] = [[12.891683578491213, 38.37789851200675], [11.765542030334474, 38.11254460084754], [12.869324684143068, 37.133463744616456], [14.642543792724611, 36.54194981843648], [15.757741928100588, 36.6001975253107], [15.576252937316896, 37.30911647598541], [15.412702560424806, 37.67132087507], [15.558614730834961, 38.02473460822767], [15.613503456115724, 38.38180096629129], [14.523024559020998, 38.243202382713605], [13.564982414245607, 38.38566957092227], [12.891683578491213, 38.37789851200675]]
    test_dict['land_only_polygon'] = [[-118.72972011566164, 34.96358310815083], [-118.7426805496216, 34.95392512349466], [-118.741851747036, 34.94128020346737], [-118.71324330568315, 34.94121204335055], [-118.7131454050541, 34.958508603969236], [-118.72972011566164, 34.96358310815083]]
    test_dict['water_only_polygon'] = [[-120.3117620944977, 32.9773526159236], [-120.35270333290102, 32.75546576141111], [-120.1521009206772, 32.67390732403642], [-119.98687148094179, 32.86846786484173], [-120.13354003429414, 33.02966016839023], [-120.3117620944977, 32.9773526159236]]
    test_dict['iceland'] = [[-24.94102478027344,66.62561451469584],[-25.57823181152344,64.39753122058228],[-21.39690399169922,62.66783857582993],[-14.134597778320314,63.92877326933141],[-12.479438781738283,65.8834234934428],[-15.170745849609377,67.10232345139119],[-20.71918487548828,66.8000257591103],[-24.94102478027344,66.62561451469584]]
    test_dict['hawaii'] = [[-179.04796600341797,30.02213803127762],[-179.18907165527344,27.3516430588189],[-154.93640899658206,17.047594180752778],[-152.70618438720703,20.787893513679396],[-176.56642913818362,30.084542946324945],[-179.04796600341797,30.02213803127762]] 
    test_dict['new_zealand'] = [[168.65386962890628,-33.35462041843625],[159.8057556152344,-46.10323266470107],[173.57917785644534,-49.428840000635226],[183.55407714843753,-37.05572508596021],[173.2358551025391,-30.40959743218008],[168.65386962890628,-33.35462041843625]]
    test_dict['caspian'] = [[50.81485748291016,47.31101290750725],[46.719017028808594,45.51151979926975],[46.25965118408204,44.07093712790448],[48.88195037841797,40.34065649361507],[48.49777221679688,37.68219008286376],[51.170883178710945,36.26766697814671],[54.774913787841804,36.636330360763424],[54.51227188110352,40.06296452858627],[53.103275299072266,40.7290474687069],[52.686481475830085,42.31260817230085],[53.21794509887696,43.012806405561534],[51.24092102050782,44.009607826541234],[51.98129653930665,45.02051982382388],[54.285507202148445,46.16746780081259],[53.84605407714844,47.512679047971524],[50.81485748291016,47.31101290750725]]
    test_dict['mkarim_aoi_test'] = [[121.60471394359236, 0.926601871146752], [121.60723686218263, 0.939203159928641], [123.02743434906007, 0.661997370761501], [123.02170505480076, 0.623377007098447], [121.60471394359236, 0.926601871146752]]
    test_dict['indonesia_standard_test'] = [[121.60723686218263,0.9392031599286415],[121.10701560974123,-1.559265273083022],[122.64930725097658,-1.8812422453465736],[122.64930725097658,-1.8869040083433015],[123.02743434906007,0.6619973707615012],[121.60723686218263,0.9392031599286415]]
    test_dict['antimeridian_test'] = [[170.85868835449222,-31.22718805085655],[184.91981506347656,-37.86319934044902],[174.5549011230469,-42.04954757896978],[170.85868835449222,-31.22718805085655]]
    test_dict['clockwise_antimeridian'] = [[164.4309997558594,-29.858510452312025],[190.13900756835938,-30.9104727678728],[165.3813171386719,-52.6130549393468],[164.4309997558594,-29.858510452312025]]
    test_dict['counterclockwise_antimeridian'] = [[190.73501586914062,-32.31499127724556],[162.78991699218753,-30.70641975748972],[160.29327392578128,-49.230153752280884],[187.09991455078128,-46.60228013300285],[190.73501586914062,-32.31499127724556]]
    test_dict['new_test_self_intersect'] = [[-61.87762962928407, 11.906899306846999], [-63.39987332594009, 12.204027982906016], [-63.452772907302574, 12.194862236528072], [-63.7737099161362, 12.012531384424141], [-64.044868, 10.641438], [-61.79129, 10.194783], [-61.528248053566784, 11.475793113716037], [-61.54109499802273, 11.510941947273063], [-61.87762962928407, 11.906899306846999]]
    result["land_only_polygon"] = [False,True,True,False,5.6,0.0,1.00,0.00]
    result["new_test_self_intersect"] = [True,True,False,False,720.4,44024.8,0.02,0.98]
    result["caspian"] = [True,True,False,False,160119.0,379902.8,0.30,0.70]
    result["iceland"] = [True,True,False,False,102765.7,111487.3,0.48,0.52]
    result["new_zealand"] = [True,True,False,False,267330.9,6214898.8,0.04,0.96]
    result["land_and_water_polygon"] = [True,True,False,False,25496.5,23308.5,0.52,0.48]
    result["hawaii"] = [True,True,False,False,16679.6,1136885.8,0.014,0.99]
    result["indonesia_standard_test"] = [True,True,False,False,6475.6,41115.8,0.14,0.86]
    result["counterclockwise_antimeridian"] = [True,True,False,False,267334.2,7647594.2,0.033,0.97]
    result["mkarim_aoi_test"] = [False,True,True,False,461.7,0.0,1.00,0.00]
    result["clockwise_antimeridian"] = [True,True,False,False,267902.0,5556725.1,0.05,0.95]
    result["antimeridian_test"] = [True,True,False,False,114487.2,10825478.1,0.01,0.99]
    result["water_only_polygon"] = [True,False,False,True,0.0,858.7,0.00,1.00]
    function_list = [covers_water, covers_land, covers_only_land, covers_only_water, get_land_area, get_water_area, get_land_percentage, get_water_percentage]
    print_list = ['Covers any water?  {:15}        {}',
                  'Covers any land?   {:15}        {}',
                  'Covers only land?  {:15}        {}',
                  'Covers only water? {:15}        {}',
                  'Land area:         {:15,.1f} km^2   {}',
                  'Water area:        {:15,.1f} km^2   {}',
                  'Land coverage:     {:15,.2f}        {}',
                  'Water coverage:    {:15,.2f}        {}']

    for name, coords in test_dict.items():
        geojson = {"type":"Polygon", "coordinates": [coords]}
        print('------------------------------------------------------')
        print('Evaluating area:   {}'.format(name))
        for i in range(len(function_list)):
            val = function_list[i](geojson)
            print(print_list[i].format(val, comparison(val, result[name][i])))

if __name__ == '__main__':
    test()
