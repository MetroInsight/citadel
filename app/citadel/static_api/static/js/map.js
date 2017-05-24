var metro = (function() {

    // global variables
    var citadelURL = 'https://citadel.ucsd.edu',
    // number of seconds of data to plot for timeseries
    timeSeriesSeconds = 24*3600,
    types = {
            /*
            'angle': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'white',
                    icon: 'compass',
                    prefix: 'fa',
                })
            },
            'airflow': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'green',
                    icon: 'flag-o',
                    prefix: 'fa',
                })
            },
            'command': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'green',
                    icon: 'terminal',
                    prefix: 'fa',
                })
            },
            'electrical_energy': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'purple',
                    icon: 'plug',
                    prefix: 'fa',
                })
            },*/
            'electrical_power': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'orange',
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
            'thermal_energy': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkpurple',
                    icon: 'battery-half',
                    prefix: 'fa'
                })
            },
            'thermal_power': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'purple',
                    icon: 'battery-half',
                    prefix: 'fa'
                })
            },/*
            'radiation': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'orange',
                    icon: 'sun-o',
                    prefix: 'fa',
                })
            },*/
            'relative_humidity': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkgreen',
                    icon: 'shower',
                    prefix: 'fa',
                })
            },/*
            'speed': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkgreen',
                    icon: 'jet-o',
                    prefix: 'fa',
                })
            },
            'time': {
                icon: L.AwesomeMarkers.icon({
                    markerColor: 'darkgreen',
                    icon: 'clock-o',
                    prefix: 'fa',
                })
            },*/            
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
    //plotControl,
    chart,
    searches = 0,
    zLayers = {
            roads: 10
    };

    function dragMoveListener (event) {
        var target = event.target,
        // keep the dragged position in the data-x/data-y attributes
        x = (parseFloat(target.getAttribute('data-x')) || 0) + event.dx,
        y = (parseFloat(target.getAttribute('data-y')) || 0) + event.dy;

        // translate the element
        target.style.webkitTransform =
            target.style.transform =
                'translate(' + x + 'px, ' + y + 'px)';

        // update the posiion attributes
        target.setAttribute('data-x', x);
        target.setAttribute('data-y', y);
    }

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

        /*
        plotControl = L.control({
            position: 'bottomleft'
        });

        plotControl.onAdd = function(map) {
            $('#plotContainer').css('display', 'block');
            var d = document.createElement('div');
            $('#plotContainer').appendTo(d);
            return d;
        };

        plotControl.addTo(map);
        */

        // TODO search on map move? 

        L.DomEvent.disableClickPropagation(document.getElementsByClassName('leaflet-control-container')[0]);

        //metro.viewTimeseries('a77c3846-6bd3-4938-8deb-ac91331135ad');

        // target elements with the "draggable" class
        interact('.draggable')
        .draggable({
            // enable inertial throwing
            inertia: true,
            // keep the element within the area of it's parent
            restrict: {
                restriction: "parent",
                endOnly: true,
                elementRect: { top: 0, left: 0, bottom: 1, right: 1 }
            },
            // enable autoScroll
            autoScroll: true,
            // call this function on every dragmove event
            onmove: dragMoveListener,
            // call this function on every dragend event
            onend: function (event) {
                var textEl = event.target.querySelector('p');

                textEl && (textEl.textContent =
                    'moved a distance of '
                    + (Math.sqrt(event.dx * event.dx +
                            event.dy * event.dy)|0) + 'px');
            }
        });/*.resizable({
            preserveAspectRatio: true,
            edges: { left: true, right: true, bottom: true, top: true }
          })
          .on('resizemove', function (event) {
            var target = event.target,
                x = (parseFloat(target.getAttribute('data-x')) || 0),
                y = (parseFloat(target.getAttribute('data-y')) || 0);

            // update the element's style
            target.style.width  = event.rect.width + 'px';
            target.style.height = event.rect.height + 'px';

            // translate when resizing from top or left edges
            x += event.deltaRect.left;
            y += event.deltaRect.top;

            target.style.webkitTransform = target.style.transform =
                'translate(' + x + 'px,' + y + 'px)';

            target.setAttribute('data-x', x);
            target.setAttribute('data-y', y);
            target.textContent = Math.round(event.rect.width) + '×' + Math.round(event.rect.height);
            //chart.setSize(null, null, false);
          });*/

        // this is used later in the resizing and gesture demos
        window.dragMoveListener = dragMoveListener;


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
            url = citadelURL + '/api/point/?geo_query:{"geometry_list":[['
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
                //console.log(d);
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
                    var match = f.properties.name.match(/(\w+)\.(\w+):(\w+)/),
                    s,
                    i;

                    if(match) {
                        s = [
                            '<b>' + match[1] + '</b>',
                            '<b>' + match[2] + '</b>',
                            '<b>' + match[3] + '</b>'
                            ];
                    } else {
                        s = ['<b>' + f.properties.name + '</b>']
                    }

                    for(i in f.properties) {
                        if(i !== 'name' && i !== 'uuid') {
                            s.push(i + ': ' + f.properties[i]);
                        }
                    }
                    s = s.join('<br/>');
                    s += '<br/><button onclick="metro.viewTimeseries(\'' 
                        + f.properties.uuid 
                        + '\')">View Timeseries</button>';
                    l.bindPopup(s);
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
                    gj.properties['uuid'] = d.point_list[i].uuid;
                    resultsLayer.addLayer(L.geoJson(gj, {
                        pointToLayer: pointToLayer,
                        onEachFeature: onEachFeature
                    }));
                }
            });
        },

        viewTimeseries: function(uuid) {

            document.getElementById('plotContainer').style.display = 'block';
            
            // get the last valid point timestamp
            $.ajax(citadelURL + '/api/point/' + uuid + '/timeseries')
            .fail(function(hqXHR, status)  {
                console.log('Error loading data: ' + status);
            }).done(function(d) {
                var i;
                if(!d.success) {
                    console.log('Failed to get timeseries.');
                }
                for(i in d.data) {
                    metro._plotTimeseries(uuid, parseInt(i) - timeSeriesSeconds, parseInt(i) + 1);
                    // should be only one value
                    break;
                }
            });

        },

        _plotTimeseries: function(uuid, start, stop) {
            // Define x-button to close time series plot
            Highcharts.SVGRenderer.prototype.symbols.cross = function (x, y, w, h) {
                return ['M', x, y, 'L', x + w, y + h, 'M', x + w, y, 'L', x, y + h, 'z'];
            };

            $.ajax(citadelURL + '/api/point/' 
                    + uuid
                    + '/timeseries?start_time='
                    + start 
                    + '&end_time='
                    + stop)
                    .fail(function(hqXHR, status)  {
                        console.log('Error loading data: ' + status);
                    }).done(function(d) {
                        var title, ytitle, type, data = [], i;
                        //console.log(d);

                        resultsLayer.eachLayer(function(l) {
                            if(l.feature.properties.uuid == uuid) {
                                //console.log(l.feature.properties);
                                title = l.feature.properties.name; 
                                type = l.feature.properties.point_type;
                                ytitle = type + ' (' + l.feature.properties.unit + ')';
                            }
                        });

                        if(!chart) {
                            chart = Highcharts.chart('plotContainer', {
                                credits: false,
                                chart: {
                                    defaultSeriesType: 'line',
                                },
                                title: {
                                    text: title
                                },
                                xAxis: {
                                    type: 'datetime',
                                    title: {
                                        text: 'Time',
                                    },
                                },
                                yAxis: {
                                    title: {
                                        text: ytitle
                                    }
                                },
                                series: [{
                                    name: type,
                                    data: []
                                }],
                                lang: {
                                    closeChart: 'Close Chart'
                                },
                                exporting: {
                                    buttons: {
                                        contextButton: {
                                            enabled: false
                                        },
                                        customButton: {
                                            x: 0,
                                            onclick: function() {
                                                chart.destroy();
                                                chart = null;
                                            },
                                            symbol: 'cross',
                                            _titleKey: 'closeChart',
                                        }
                                    }
                                }
                            });
                        } else {
                            chart.series[0].update({
                                name: type
                            });
                            chart.setTitle({
                                text: title
                            });                        
                            chart.yAxis[0].setTitle({
                                text: ytitle
                            });
                        }

                        for(i in d.data) {
                            // convert from seconds to milliseconds
                            data.push([i*1000,d.data[i]]);
                        }
                        chart.series[0].setData(data);
                    });
        }  
    }

})();
