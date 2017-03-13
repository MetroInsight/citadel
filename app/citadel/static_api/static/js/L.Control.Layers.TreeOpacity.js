//
// Copyright (c) 2016 The Regents of the University of California.
// All rights reserved.
//
// Permission is hereby granted, without written agreement and without
// license or royalty fees, to use, copy, modify, and distribute this
// software and its documentation for any purpose, provided that the above
// copyright notice and the following two paragraphs appear in all copies
// of this software.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
// ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
// THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE
// PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF
// CALIFORNIA HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
// ENHANCEMENTS, OR MODIFICATIONS.
//
/**
 * Leaflet plugin to allow nested items in the layers control. Some code originated
 * in L.Control.Layers.js.
 * 
 * @author Daniel Crawl
 * @version $$Id: L.Control.Layers.TreeOpacity.js 1022 2017-01-10 17:56:12Z crawl $$
 */

L.Control.Layers.TreeOpacity = L.Control.Layers.extend({

	_addLayer: function (layer, name, overlay, child) {

		//console.log(name + " " + layer.hasOwnProperty('isNestedLayer'));
		
		var id = L.stamp(layer);

		this._layers[id] = {
			layer: layer,
			name: name,
			overlay: overlay,
			children: [],
			child: child
		};

		if (this.options.autoZIndex && layer.setZIndex) {
			this._lastZIndex++;
			layer.setZIndex(this._lastZIndex);
		}
		
		if(typeof layer.onAdd !== 'function') {
			for(var i in layer) {
				var cid = this._addLayer(layer[i], i, overlay, true);
				this._layers[id].children.push(cid);
			}
		}
		
		return id;
		
	},

	_update: function () {
		if (!this._container) {
			return;
		}

		this._baseLayersList.innerHTML = '';
		this._overlaysList.innerHTML = '';

		var baseLayersPresent = false,
		    overlaysPresent = false,
		    i, obj;

		for (i in this._layers) {
			obj = this._layers[i];
			if(!obj.child) {
				this._addItem(obj);
			}
			overlaysPresent = overlaysPresent || obj.overlay;
			baseLayersPresent = baseLayersPresent || !obj.overlay;
		}

		this._separator.style.display = overlaysPresent && baseLayersPresent ? '' : 'none';
	},

	_addItem: function (obj) {
		var label = document.createElement('label'),
		    input,
		    checked = this._map.hasLayer(obj.layer),
		    range;
		
		if (obj.overlay) {
			input = document.createElement('input');
			input.type = 'checkbox';
			input.id = input.name = obj.name;
			if(obj.children.length > 0) {
				input.className = 'leaflet-control-layers-selector-parent';
			} else {
				input.className = 'leaflet-control-layers-selector';
				//input.className = 'leaflet-control-layers-selector-child';
			}
			input.defaultChecked = checked;
			
			if(this.options.opacity && obj.layer.setOpacity) {
				range = document.createElement('input');
				range.type = 'range';
				range.min = 0.0;
				range.max = 1.0;
				range.step = 0.01;
				if(obj.layer.getOpacity) {
				    range.value = obj.layer.getOpacity();
				} else {
				    range.value = 1;
				}
				range.oninput = range.onchange = function(r) {
					obj.layer.setOpacity(this.value);
				};
			}
		} else {
			input = this._createRadioElement('leaflet-base-layers', checked);
		}

		input.layerId = L.stamp(obj.layer);
		
		L.DomEvent.on(input, 'click', this._onInputClick, this);

		var name = document.createElement('span');
		name.innerHTML = ' ' + obj.name;

		label.appendChild(input);
		if(obj.overlay && obj.children.length > 0) {
			var l2 = document.createElement('label');
			l2.htmlFor = obj.name;
			label.appendChild(l2);
			
			if(L.Browser.ie || !L.Browser.webkit) {
				l2.style['background-position'] = "1px";
				l2.style.padding = "0 5 0 0px";
			}

		}
		label.appendChild(name);
		
		if(range) {
			label.appendChild(range);
		}

		var container = obj.overlay ? this._overlaysList : this._baseLayersList;
		container.appendChild(label);

		return label;
	},

	_onInputClick: function () {
		var i, input, obj,
		    inputs = this._form.getElementsByTagName('input'),
		    inputsLen = inputs.length;

		this._handlingClick = true;

		for (i = 0; i < inputsLen; i++) {
			input = inputs[i];
			if(input) {
				if(input.type != 'range') {
					obj = this._layers[input.layerId];
		
					if(obj.children.length > 0) {
	
						var children = input.parentNode.childNodes;
						var j;
						
						if(input.checked) {
							// show children
							
							var found = false;
							for(j in children) {
								if(children[j].nodeName === 'UL') {
									found = true;
									break;
								}
							}
							
							if(!found) {
								var ul = document.createElement('ul');
								ul.className = 'leaflet-control-ul';
								input.parentNode.appendChild(ul);
								for(j in obj.children) {
									var childlayer = this._layers[obj.children[j]];
									if(childlayer.name !== '_leaflet_id') {
										var li = document.createElement('li');
										var item = this._addItem(childlayer);
										li.appendChild(item);
										ul.appendChild(li);
									}
								}
							}
						} else {
							// hide children
							for(j in children) {
								if(children[j].nodeName === 'UL') {
									input.parentNode.removeChild(children[j]);
									break;
								}
							}
						}
					} else {
						if (input.checked && !this._map.hasLayer(obj.layer)) {
							this._map.addLayer(obj.layer);
			
						} else if (!input.checked && this._map.hasLayer(obj.layer)) {
							this._map.removeLayer(obj.layer);
						}
					}
				}
			}
		}

		this._handlingClick = false;

		this._refocusOnMap();
	},

});

L.control.layers.treeopacity = function (baseLayers, overlays, options) {
	return new L.Control.Layers.TreeOpacity(baseLayers, overlays, options);
};