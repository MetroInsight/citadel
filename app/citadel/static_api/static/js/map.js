var metro = (function() {

    // global variables used in functions
    var types = {
            'electrical power': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'purple',
                    icon: 'plug',
                    prefix: 'fa',
                })
            },
            'temperature': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkred',
                    icon: 'thermometer-full',
                    prefix: 'fa'
                })
            },
            'thermal energy': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkpurple',
                    icon: 'battery-half',
                    prefix: 'fa'
                })
            },
            'thermal power': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkpurple',
                    icon: 'flash',
                    prefix: 'fa'
                })
            },
            'waterflow': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'blue',
                    icon: 'tint',
                    prefix: 'fa'
                })
            }
    },
    map,
    mapLayers,
    resultsLayer,
    searches = 0,
    zLayers = {
            roads: 10
    };

    document.onready = function() {

        var Esri_WorldStreetMap = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}', {
            attribution: 'Tiles &copy; Esri &mdash; Source: Esri, DeLorme, NAVTEQ, USGS, Intermap, iPC, NRCAN, Esri Japan, METI, Esri China (Hong Kong), Esri (Thailand), TomTom, 2012',
            maxZoom: 18
        });

        var Esri_WorldImagery = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
            maxZoom: 18
        });

        var Stamen_TonerHybrid = L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/toner-hybrid/{z}/{x}/{y}.{ext}', {
            attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
            subdomains: 'abcd',
            minZoom: 0,
            maxZoom: 16,
            ext: 'png',
            zIndex: zLayers.roads,
        });

        map = new L.Map('map', {
            layers: [Esri_WorldStreetMap],
            zoomControl: false,
            // zoom: 8,
            // center: L.latLng(33.580865, -118.134080),
            center : L.latLng(32.87761,-117.245),
            zoom : 15,
            // disable keyboard panning since may be pressing up/down to change
            // image in chart.
            keyboard: false
        });

        mapLayers =  L.control.layers.treeopacity({
            "Streets": Esri_WorldStreetMap,
            "Satellite": Esri_WorldImagery
        }, {
        }, {
            autoZIndex: false,
            collapsed: true,
            opacity: true
        }).addTo(map);

        L.control.mousePosition({
            position: 'bottomright'
        }).addTo(map);

        L.control.scale({
            position: 'bottomright'
        }).addTo(map);

        var query = L.control({
            position: 'topleft'
        });

        query.onAdd = function(map) {
            $('#queryContainer').css('display', 'block');
            var d = document.createElement('div');
            $('#queryContainer').appendTo(d);
            return d;
        };

        query.addTo(map);

        map.on('baselayerchange', function(e) {
            if(e.layer == Esri_WorldImagery) {
                map.addLayer(Stamen_TonerHybrid);
            } else {
                map.removeLayer(Stamen_TonerHybrid);                        
            }
        });

        var o, d, i;

        var typeSelect = document.getElementById('typeId');
        for(i in types) {
            o = document.createElement("option");
            o.value = o.text = i;
            typeSelect.add(o);
        }

        //t.options[0].selected = true;

        $('#typeId').multiselect({
            columns: 2,
            selectAll: true,
            texts: {
                placeholder: 'Select dataset type'
            },
            onOptionClick: function(e, o) {
                for(i in typeSelect.options) {
                    if(typeSelect.options[i].value === o.value) {
                        if(typeSelect.options[i].selected) {
                            metro.search(o.value);
                        } else {
                            metro.remove(o.value);
                        }
                        break;
                    }
                }
            }
        });

        resultsLayer = L.markerClusterGroup().addTo(map);

        // TODO search on map move? 

        L.DomEvent.disableClickPropagation(document.getElementsByClassName('leaflet-control-container')[0]);

    };

    return {

        remove: function(type) {
            resultsLayer.eachLayer(function(l) {
                if(l.feature.properties.point_type == type) {
                    resultsLayer.removeLayer(l);
                }
            });
        },
        
        search: function(type) {
            var bounds = map.getBounds(),
            url = 'http://citadel.ucsd.edu/api/point/?geo_query:{"geometry_list":[['
                + bounds.getSouthWest().lng + ',' + bounds.getSouthWest().lat 
                + '],['  
                + bounds.getNorthEast().lng + ',' + bounds.getNorthEast().lat 
                + ']],"type":"bounding_box"}' 
                + '&tag_query={"point_type":"' + type + '"}',
                i;

            //console.log(url);
           
            searches++;
            if(searches == 1) {
                map.spin(true);
            }

            $.ajax(url)
            .always(function() {
                searches--;
                if(searches == 0) {
                    map.spin(false);
                }
            }).fail(function(hqXHR, status)  {
                console.log('Error loading data: ' + status);
            }).done(function(d) {
                // console.log(d);
                var pointToLayer = function(f, ll) {
                    if(types[f.properties.point_type]) {
                        return L.marker(ll, {
                            icon: types[f.properties.point_type].icon
                        });
                    } else {
                        console.log('unknown type of point_type: ' + f.properties.point_type);
                        return L.marker(ll);
                    }
                };

                var onEachFeature = function(f, l) {
                    var s = ['<b>' + f.properties.name + '</b>'],
                    i;
                    for(i in f.properties) {
                        if(i !== 'name') {
                            s.push(i + ': ' + f.properties[i]);
                        }
                    }
                    l.bindPopup(s.join('<br/>'));
                };

                var i, j, gj;

                for(i in d.point_list) {
                    gj = {
                            type: 'Feature',
                            geometry: d.point_list[i].geometry,
                            properties: {
                                name: d.point_list[i].name
                            }
                    };
                    for(j in d.point_list[i].tags) {
                        gj.properties[j] = d.point_list[i].tags[j];
                    }
                    resultsLayer.addLayer(L.geoJson(gj, {
                        pointToLayer: pointToLayer,
                        onEachFeature: onEachFeature
                    }));
                }
            });
        }
    }

})();
