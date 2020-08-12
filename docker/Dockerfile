FROM hysds/isce2:latest-es1

MAINTAINER mkarim "Mohammed.R.Karim@jpl.nasa.gov"
LABEL description="standard_product"

ARG git_oauth_token
ARG id
ARG gid


RUN set -ex \
 && . /opt/conda/bin/activate root \
 && $HOME/verdi/bin/pip install opencv-python \
 #&& sudo yum install -y libgeos-3.6.2 \
 && sudo yum install -y https://download1.rpmfusion.org/free/el/rpmfusion-free-release-7.noarch.rpm \
 && sudo yum install -y geos-devel \
 && $HOME/verdi/bin/pip install --user https://github.com/matplotlib/basemap/archive/master.zip
 #&& $HOME/verdi/bin/pip install https://github.com/matplotlib/basemap/archive/v1.1.0.tar.gz


# as ops user
USER ops
#RUN set -ex && \
 # usermod -u ${id} ops && \
  #groupmod -g ${gid} ops && \
  #chown -R ops:ops /home/ops/ /data && \
  #usermod -g ${gid} ops

# copy config
#COPY .aws /home/ops/.aws
#COPY .netrc /home/ops/.netrc

# copy packages
COPY . /home/ops/verdi/ops/standard_product

#RUN /opt/conda/bin/conda install --yes basemap

USER ops
RUN $HOME/verdi/bin/pip install shapely
RUN $HOME/verdi/bin/pip install 'fiona==1.7.13'
RUN $HOME/verdi/bin/pip install 'pyproj==1.9.6'
RUN $HOME/verdi/bin/pip install 'geopandas==0.6.3'
RUN $HOME/verdi/bin/pip install elasticsearch
#RUN $HOME/verdi/bin/pip install --user git+https://github.com/matplotlib/basemap.git
#RUN $HOME/verdi/bin/pip install geos
#RUN yum install libgeos-dev
#RUN yum install https://download1.rpmfusion.org/free/el/rpmfusion-free-release-7.noarch.rpm

#user root

#RUN yum install http://download-ib01.fedoraproject.org/pub/epel/7/x86_64/
#RUN rpm -Uvh epel-release*rpm
#RUN yum install geos-devel
#RUN $HOME/verdi/bin/pip install --user https://github.com/matplotlib/basemap/archive/master.zip
#RUN /opt/conda/bin/conda install --yes basemap opencv
#RUN ln -s /opt/conda/pkgs/basemap-*/share/basemap $HOME/verdi/share/basemap


user ops
# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data


RUN set -ex \
 && sudo chown -R ops:ops /home/ops/verdi/ops/standard_product \
 && mv /home/ops/verdi/ops/standard_product/multi_acquisition_localizer /home/ops/verdi/ops/ \
 && mv /home/ops/verdi/ops/standard_product/ariamh/ /home/ops/ 



WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
