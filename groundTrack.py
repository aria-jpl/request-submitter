#!/usr/bin/env python3

from builtins import zip
from builtins import str
import isce
import isceobj
import xml.etree.ElementTree as ET
import datetime
import os
import glob
from isceobj.Orbit.Orbit import Orbit, StateVector
from isceobj.Util.Poly2D import Poly2D
import numpy as np
from isceobj.Planet.Planet import Planet
from isceobj.Sensor.TOPS.Sentinel1 import Sentinel1 as S1
from isceobj.Sensor.TOPS.Sentinel1 import s1_findOrbitFile
from isceobj.Sensor.TOPS.BurstSLC import BurstSLC
import shapely
from shapely.geometry import Polygon, mapping

import pyproj    
import shapely
import shapely.ops as ops
from shapely.geometry.polygon import Polygon
from functools import partial
import traceback

def extractPreciseOrbit(sentinel1, margin=60.0):
    '''
        Extract precise orbit from given Orbit file.
    '''
    try:
        try:
            fp = open(sentinel1.orbitFile,'r')
        except IOError as strerr:
            print("IOError: %s" % strerr)
            traceback.print_exc()
            return False
 
        _xml_root = ET.ElementTree(file=fp).getroot()
 
        node = _xml_root.find('Data_Block/List_of_OSVs')
 
        print('Extracting orbit from Orbit File: ', sentinel1.orbitFile)
        orb = Orbit()
        orb.configure()
 
        margin = datetime.timedelta(seconds=margin)
        print("sentinel1.product.bursts : %s" %sentinel1.product.bursts)
        tstart = sentinel1.product.bursts[0].sensingStart - margin
        tend = sentinel1.product.bursts[-1].sensingStop + margin
 
        for child in node.getchildren():
            timestamp = sentinel1.convertToDateTime(child.find('UTC').text[4:])
 
            if (timestamp >= tstart) and (timestamp < tend):
 
                ###Warn if state vector quality is not nominal
                quality = child.find('Quality').text.strip()
                if quality != 'NOMINAL':
                    print('WARNING: State Vector at time {0} tagged as {1} in orbit file {2}'.format(timestamp, quality, sentinel1.orbitFile))
                    print("Excluding the date data")
                    return False
    except Exception as err:
        print("extractPreciseOrbit Error : %s" %str(err))
        traceback.print_exc()
        return False

    return True



def isValidOrbit(tstart,tend, mission, orbitFile=None,orbitDir=None):

    orbitFile = os.path.basename(orbitFile)
    print("groundTrack : isValidOrbit: %s, %s, %s, %s, %s " %(tstart, tend, mission, orbitFile, orbitDir))

    # initiate a Sentinel-1 product instance
    sentinel1 = S1()
    sentinel1.configure()
    
    # add information on orbit file or orbit directory
    if orbitFile is None and orbitDir is None:
        raise Exception("Either provide the information of the orbitFile or orbitDir")
    if orbitFile is not None:
        # orbit file is specified, will directly feed this into the Sentinel-1 product
        sentinel1.orbitFile = os.path.join(orbitDir, orbitFile)
    else:
        # orbit dir is specified, will directly feed this into the Sentinel-1 product
        sentinel1.orbitDir=orbitDir

        # search the directory for the correct orbit file
        sentinel1.orbitFile = s1_findOrbitFile(orbitDir,tstart,tend, mission)

    # ISCE internals read the required time-period to be extracted from the orbit using the sentinel-1 product start and end-times.
    # Below we will add a dummy burst with the user-defined start and end-time and include it in the sentinel-1 product object.
   
    print("Orbit File : %s" %orbitFile) 
    # Create empty burst SLC
    burst = []
    burst1 = BurstSLC()
    burst1.configure()
    burst1.burstNumber = 1
    burst.append(burst1)
    
    # adding the start and end time
    burst[0].sensingStart=tstart
    burst[0].sensingStop=tend
    
    # add SLC burst to product
    sentinel1.product.bursts = burst

    # extract the precise orbit information into an orb variable
    #orb = sentinel1.extractPreciseOrbit()
    return extractPreciseOrbit(sentinel1)

def S1orbit(tstart,tend, mission, orbitFile=None,orbitDir=None):
    '''Function that will extract the sentinel-1 state vector information from the 
       orbit files and populate a ISCE sentinel-1 product with the state vector information.'''

    # initiate a Sentinel-1 product instance
    sentinel1 = S1()
    sentinel1.configure()
    
    # add information on orbit file or orbit directory
    if orbitFile is None and orbitDir is None:
        raise Exception("Either provide the information of the orbitFile or orbitDir")
    if orbitFile is not None:
        # orbit file is specified, will directly feed this into the Sentinel-1 product
        sentinel1.orbitFile = os.path.join(orbitDir, orbitFile)
    else:
        # orbit dir is specified, will directly feed this into the Sentinel-1 product
        sentinel1.orbitDir=orbitDir

        # search the directory for the correct orbit file
        sentinel1.orbitFile = s1_findOrbitFile(orbitDir,tstart,tend, mission)

    # ISCE internals read the required time-period to be extracted from the orbit using the sentinel-1 product start and end-times.
    # Below we will add a dummy burst with the user-defined start and end-time and include it in the sentinel-1 product object.
   
    print("Orbit File : %s" %orbitFile) 
    # Create empty burst SLC
    burst = []
    burst1 = BurstSLC()
    burst1.configure()
    burst1.burstNumber = 1
    burst.append(burst1)
    
    # adding the start and end time
    burst[0].sensingStart=tstart
    burst[0].sensingStop=tend
    
    # add SLC burst to product
    sentinel1.product.bursts = burst

    # extract the precise orbit information into an orb variable
    orb = sentinel1.extractPreciseOrbit()
    
    # add the state vector information ot the burst SLC product
    for sv in orb:
        burst1.orbit.addStateVector(sv)

    return burst1
    

def topo(burst,time,Range,doppler=0,wvl=0.056):
    '''Function that return the lon lat information for a given time, range, and doppler'''
    
    ###Planet parameters
    elp = Planet(pname='Earth').ellipsoid
    
    # Provide a zero doppler polygon in case 0 is given
    if doppler is 0:
        doppler = Poly2D()
        doppler.initPoly(rangeOrder=1, azimuthOrder=0, coeffs=[[0, 0]])

    # compute the lonlat grid
    latlon = burst.orbit.rdr2geo(time,Range,doppler=doppler, wvl=wvl)
    return latlon

def plotresults(latlon_outline,satpath):
    
    
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt
    from matplotlib.patches import Polygon

    # make a map of the world
    fig = plt.figure(figsize=(11,11))

    mmap = Basemap(projection='cyl')
    mmap.drawmapboundary(fill_color='aqua')
    mmap.fillcontinents(color='green', lake_color='aqua')
    mmap.drawcoastlines()

    # plotting the track outline
    lat, lon = mmap(latlon_outline[:,1], latlon_outline[:,0])
    latlon_outline=list(zip(lat,lon))
    track_outline = Polygon( latlon_outline, facecolor='blue', edgecolor='blue', alpha=0.2)
    plt.gca().add_patch(track_outline)
    
    # plotting the ground trace of satellite
    satx, saty = mmap(satpath[:,1], satpath[:,0])
    mmap.plot(satx,saty,color='k')


def get_plot_data(latlon_outline,satpath):
    from mpl_toolkits.basemap import Basemap
    mmap = Basemap(projection='cyl')
    lat, lon = mmap(latlon_outline[:,1], latlon_outline[:,0])
    latlon_outline=list(zip(lat,lon))
    #print("latlon_outline : %s" %latlon_outline)
    #track_outline = Polygon( latlon_outline, facecolor='blue', edgecolor='blue', alpha=0.2)
    track_outline = Polygon( latlon_outline)

    #print("track_outline : %s" %track_outline)
    return latlon_outline
    #return track_outline
    

def get_ground_track(tstart, tend, mission, orbit_file, orbitDir): 


    
    # generating an Sentinel-1 burst dummy file populated with state vector information for the requested time-period
    burst = S1orbit(tstart,tend,mission,orbit_file, orbitDir)
    orbit_file = os.path.basename(orbit_file)
    print("groundTrack : get_ground_track: %s, %s, %s, %s, %s " %(tstart, tend, mission, orbit_file, orbitDir))

    # constants for S1
    nearRange = 800e3 #Near range in m
    farRange = 950e3  #Far range in m
    doppler = 0       # zero doppler
    wvl = 0.056       # wavelength
    
    # sampling the ground swath (near and far range) in 10 samples
    latlon_nearR = []
    latlon_farR = []
    satpath = []
    #latlon_geoms = []
    delta = (tend - tstart).seconds
    print("delta : %s" %delta)
    #deltat = np.linspace(0,1, num=int(delta/2))
    deltat = np.linspace(0,1, num=delta)
    elp = Planet(pname='Earth').ellipsoid
    for tt in deltat:
        tinp = tstart + tt * (tend-tstart)

        latlon_nearR_pt = topo(burst,tinp,nearRange,doppler=doppler,wvl=wvl)
        #print("latlon_nearR_pt : %s " %latlon_nearR_pt)
        #latlon_nearR.append([latlon_nearR_pt[0], latlon_nearR_pt[1]])
        latlon_farR_pt = topo(burst,tinp,farRange,doppler=doppler,wvl=wvl)
        #latlon_farR.append([latlon_farR_pt[0], latlon_farR_pt[1]])
        #print("latlon_farR_pt : %s " %latlon_farR_pt)        
        latlon_nearR.append(topo(burst,tinp,nearRange,doppler=doppler,wvl=wvl))
        latlon_farR.append(topo(burst,tinp,farRange,doppler=doppler,wvl=wvl))
        satpath.append(elp.xyz_to_llh(burst.orbit.interpolateOrbit(tinp, method='hermite').getPosition()))
        #latlon_geoms.append( [latlon_nearR_pt, latlon_farR_pt])

    latlon_nearR = np.array(latlon_nearR)
    latlon_farR = np.array(latlon_farR)
    satpath = np.array(satpath)

    # flip one side such that a polygon can be made by concatenating both.
    latlon_farR=np.flipud(latlon_farR)
    latlon_outline = np.vstack([latlon_nearR,latlon_farR])
    
    #print("latlon_outline : %s\n" %latlon_outline)
    #print("satpath : %s END\n" %satpath)
    #print("latlon_geoms : %s\n" %latlon_geoms)
    # plotting the results
    return get_plot_data(latlon_outline,satpath)

    #return latlon_outline
