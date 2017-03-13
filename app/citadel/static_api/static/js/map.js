var metro = (function() {

    // constants
    var icons = {
            'electrical power': L.AwesomeMarkers.icon({
                markerColor: 'purple',
                icon: 'plug',
                prefix: 'fa',
            }),
            'temperature': L.AwesomeMarkers.icon({
                markerColor: 'darkred',
                icon: 'thermometer-full',
                prefix: 'fa'
            }),
            'thermal energy': L.AwesomeMarkers.icon({
                markerColor: 'darkpurple',
                icon: 'battery-half',
                prefix: 'fa'
            }),
            'thermal power': L.AwesomeMarkers.icon({
                markerColor: 'darkpurple',
                icon: 'flash',
                prefix: 'fa'
            }),
            'waterflow': L.AwesomeMarkers.icon({
                markerColor: 'blue',
                icon: 'tint',
                prefix: 'fa'
            }),
    },
    // global variables used in functions
    map,
    mapLayers,
    resultsLayer,
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

        // plugin for selectize so that enter key performs search
        Selectize.define('enter_key_submit', function (options) {
            var self = this;

            this.onKeyDown = (function (e) {
                var original = self.onKeyDown;

                return function (e) {
                    // this.items.length MIGHT change after event propagation.
                    // We need the initial value as well. See next comment.
                    var initialSelection = this.items.length;
                    original.apply(this, arguments);

                    if (e.keyCode === 13
                            // Necessary because we don't want this to be triggered when an option is selected with Enter after pressing DOWN key to trigger the dropdown options
                            && initialSelection && initialSelection === this.items.length
                            && this.$control_input.val() === '') {
                        self.trigger('submit');
                    }
                };
            })();
        });

        var search = $('#search').selectize({
            delimiter: ',',
            persist: false,
            create: true,
            plugins: ['enter_key_submit'],
            onInitialize: function () {
                this.on('submit', function () {
                    //this.$input.closest('form').submit();
                    metro.search();
                }, this);
            },
        });

        resultsLayer = L.markerClusterGroup().addTo(map);

        document.getElementById('searchButton').addEventListener('click', function() {
            metro.search();
        });

        // enable button in case cached as disabled
        document.getElementById('searchButton').disabled = false;
        
        // TODO search on map move? 
        
        L.DomEvent.disableClickPropagation(document.getElementsByClassName('leaflet-control-container')[0]);

    };

    return {

        search: function() {
            var url,
            bounds = map.getBounds(),
            types, i;

            document.getElementById('searchIcon').className = 'fa fa-spin fa-spinner';
            document.getElementById('searchButton').disabled = true;

            resultsLayer.clearLayers();
            
            url = 'http://citadel.ucsd.edu/api/point/?geo_query:{"geometry_list":[[' +
                bounds.getSouthWest().lng + ',' + bounds.getSouthWest().lat +
                '],[' + 
                bounds.getNorthEast().lng + ',' + bounds.getNorthEast().lat +
                ']],"type":"bounding_box"}';

            types = document.getElementById('search').value.split(/\s*,\s*/)
            if(types.length > 1 || types[0].length > 0) {
                for(i in types) {
                    metro._doSearch(url + '&tag_query={"point_type":"' + types[i] + '"}');
                }
            } else {
                metro._doSearch(url);
            }

        },
        
        _doSearch: function(url) {
            
            console.log(url);

            $.ajax(url)
            .always(function() {
                document.getElementById('searchIcon').className = 'fa fa-search';
                document.getElementById('searchButton').disabled = false;
            }).fail(function(hqXHR, status)  {
                console.log('Error loading data: ' + status);
            }).done(function(d) {
                // console.log(d);

                var pointToLayer = function(f, ll) {
                    if(icons[f.properties.point_type]) {
                        return L.marker(ll, {
                            icon: icons[f.properties.point_type]
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

                // console.log(fc);
                // group.addTo(map);
            });
        }
    }

})();
