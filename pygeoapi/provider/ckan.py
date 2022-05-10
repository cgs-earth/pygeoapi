# =================================================================
#
# Authors: Benjamin Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2022 Benjamin Webb
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import json
from json.decoder import JSONDecodeError
from requests import Session, codes
import logging

from pygeoapi.provider.base import (BaseProvider, ProviderQueryError,
                                    ProviderConnectionError)

LOGGER = logging.getLogger(__name__)


class CKANServiceProvider(BaseProvider):
    """CKAN API Provider
    """

    def __init__(self, provider_def):
        """
        CKAN Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data, id_field, name set in parent class

        :returns: pygeoapi.provider.esri.ESRIServiceProvider
        """
        LOGGER.debug("Logger CKAN Init")

        super().__init__(provider_def)
        self.resource_id = provider_def['resource_id']
        self.get_fields()

    def get_fields(self):
        """
        Get fields of CKAN Provider

        :returns: dict of fields
        """

        if not self.fields:
            # Start session
            s = Session()

            # Load fields
            params = {
                'resource_id': self.resource_id,
                'limit': 1,
                'include_total': 'true'
            }

            if self.properties:
                self.properties = \
                    set(self.properties) \
                    | set([self.id_field, self.x_field, self.y_field])
                params['fields'] = ','.join(self.properties)

            try:
                with s.get(self.data, params=params) as r:
                    resp = r.json()
                    LOGGER.error(r.url)
                    self.fields = {
                        _.pop('id'): _ for _ in resp['result']['fields']
                    }
                    self.numFeatures = resp['result']['total']

            except JSONDecodeError as err:
                LOGGER.error('Bad response at {}'.format(self.data))
                raise ProviderQueryError(err)

        return self.fields

    def query(self, offset=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None, **kwargs):
        """
        CKAN query

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of GeoJSON FeatureCollection
        """

        return self._load(offset, limit, resulttype, bbox=bbox,
                          datetime_=datetime_, properties=properties,
                          sortby=sortby, select_properties=select_properties,
                          skip_geometry=skip_geometry)

    def get(self, identifier, **kwargs):
        """
        Query CKAN by id

        :param identifier: feature id

        :returns: dict of single GeoJSON feature
        """

        fc = self._load(identifier=identifier)
        return fc.get('features').pop()

    def _load(self, offset=0, limit=10, resulttype='results',
              identifier=None, bbox=[], datetime_=None, properties=[],
              sortby=[], select_properties=[], skip_geometry=False, q=None):
        """
        Private function: Load ESRI data

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param identifier: feature id (get collections item)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of GeoJSON FeatureCollection
        """

        # Default feature collection and request parameters
        fc = {
            'type': 'FeatureCollection',
            'features': []
        }
        params = {
            'offset': offset,
            'limit': limit,
            'resource_id': self.resource_id
        }

        if self.properties or select_properties:
            params['fields'] = ','.join(
                set(self.properties) | set(select_properties))

        if identifier:
            # Add feature id to request params
            properties = [(self.id_field, identifier), ]
            params['filters'] = self._make_where(properties)

        else:
            # Add queryables to request params
            if properties or bbox:
                params['filters'] = self._make_where(properties)

            if resulttype == 'hits':
                params['include_total'] = 'true'

            if sortby:
                params['sort'] = self._make_orderby(sortby)

        # Start session
        s = Session()

        # Form URL for GET request
        LOGGER.debug('Sending query')
        with s.get(self.data, params=params) as r:

            if r.status_code == codes.bad:
                LOGGER.error('Bad http response code')
                raise ProviderConnectionError('Bad http response code')

            resp = r.json()

            limit = resp['result']['limit']

        if resulttype == 'hits':
            # Return hits
            LOGGER.debug('Returning hits')
            fc['numberMatched'] = resp['result']['total']

        else:
            # Return feature collection
            v = [self.make_feature(f, skip_geometry)
                 for f in resp['result']['records']]

            step = len(v)
            hits_ = min(limit, self.numFeatures)

            # Query if values are less than expected
            while len(v) < hits_:
                LOGGER.debug('Fetching next set of values')
                params['offset'] += step
                with s.get(self.data, params=params) as r:

                    LOGGER.debug(r.url)
                    if r.status_code == codes.bad:
                        LOGGER.error('Bad http response code')
                        raise ProviderConnectionError('Bad http response code')

                    resp = r.json()

                    _ = [self.make_feature(f, skip_geometry)
                         for f in resp['result']['records']]

                    if len(_) == 0:
                        break
                    else:
                        v.extend(_)

            fc['features'] = v
            fc['numberReturned'] = len(v)

        # End session
        s.close()

        return fc

    def make_feature(self, f, skip):
        feature = {'type': 'Feature'}

        feature['id'] = f.pop(self.id_field)

        if not skip:
            feature['geometry'] = {
                'type': 'Point',
                'coordinates': [
                    float(f.pop(self.x_field)),
                    float(f.pop(self.y_field))
                ]
            }
        else:
            feature['geometry'] = None

        feature['properties'] = f

        return feature

    @staticmethod
    def _make_orderby(sortby):
        """
        Private function: Make ESRI filter from query properties

        :param sortby: `list` of dicts (property, order)

        :returns: ESRI query `order` clause
        """
        __ = {'+': 'asc', '-': 'desc'}
        ret = [f"{_['property']} {__[_['order']]}" for _ in sortby]

        return ','.join(ret)

    def _make_where(self, properties, bbox=[]):
        """
        Private function: Make ESRI filter from query properties

        :param properties: `list` of tuples (name, value)
        :param bbox: bounding box [minx,miny,maxx,maxy]

        :returns: ESRI query `where` clause
        """

        p = {}

        if properties:
            p.update(
                {k: v for (k, v) in properties}
            )

        return json.dumps(p)

    def __repr__(self):
        return '<ESRIProvider> {}'.format(self.data)
