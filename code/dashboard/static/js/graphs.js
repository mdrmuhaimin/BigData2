queue()
    // .defer(d3.json, "/falldetection/dashboard")
    .defer(d3.csv, "/getfile")
    .await(makeGraphs);

function makeGraphs(error, data) {
	
	function print_filter(filter) {
	    var f=eval(filter);
	    if (typeof(f.length) != "undefined") {}else{}
	    if (typeof(f.top) != "undefined") {f=f.top(Infinity);}else{}
	    if (typeof(f.dimension) != "undefined") {f=f.dimension(function(d) { return "";}).top(Infinity);}else{}
	    console.log(filter+"("+f.length+") = "+JSON.stringify(f).replace("[","[\n\t").replace(/}\,/g,"},\n\t").replace("]","\n]"));
	}

	data.forEach(function(d) {
	    d["time_seconds"] = +d["time_seconds"];
		d["reading"] = +d["reading"];
		d["x"] = +d["x"];
		d["y"] = +d["y"];

	});

	var facts = crossfilter(data);
	var timesecondsDim = facts.dimension(function(d) { return d.time_seconds; });
	var trial_typeDim = facts.dimension(function(d) { return d.trial_type;});
	var trial_subtypeDim = facts.dimension(function(d) { return d.trial_subtype;});
	var measureDim = facts.dimension(function(d) { return d.measure;});
	var scatterDimension = facts.dimension(function(d){ return [d["x"], d["y"], d['sensor']];});
	var scatterGroup = scatterDimension.group().reduceCount();


	var readingForAxis = function(axis) {
	  return function(d) {
	    return d.axis === axis ? +d.reading : null;
	  };
	};

	var reading_x_Group = timesecondsDim.group().reduceSum(readingForAxis('X'));
	var reading_y_Group = timesecondsDim.group().reduceSum(readingForAxis('Y'));
	var reading_z_Group = timesecondsDim.group().reduceSum(readingForAxis('Z'));
	var reading_xyz_Group = timesecondsDim.group().reduceSum(readingForAxis('XYZ'));

	function remove_empty_bins(source_group) {
	    return {
	        all:function () {
	            return source_group.all().filter(function(d) {
	                return Math.abs(d.value) > 0.000001; // if using floating-point numbers
	                // return d.value !== 0; // if integers only
	            });
	        }
	    };
	}


  	select_measure = dc.selectMenu("#select-measure")
 	    .dimension(measureDim)
	    .group(measureDim.group())
	    .controlsUseVisibility(true)
	    .filter('acceleration')
	    .title(function (d){
	    	return d.key;
	    });

  	select_trial_type = dc.selectMenu("#select-trial-type")
 	    .dimension(trial_typeDim)

	    .group(trial_typeDim.group())
	    .controlsUseVisibility(true)
	    .filter('ADLs')
	    .title(function (d){
	    	return d.key;
	    });

  	select_trial_subtype = dc.selectMenu("#select-trial-subtype")
 	    .dimension(trial_subtypeDim)
	    .group(trial_subtypeDim.group())
	    .controlsUseVisibility(true)
	    .filter('AS')
	    .title(function (d){
	    	return d.key;
	    });


	var bubble = dc.bubbleChart("#bubble-chart")
		.width(200)
		.height(500)
		.margins({top:10,bottom:60,right:40,left:60})
		.dimension(scatterDimension)
		.group(scatterGroup)
		.colors(d3.scale.category10())
		.keyAccessor(function(d) { return d.key[0] ;})
		.valueAccessor(function(d) { return d.key[1];})
		.radiusValueAccessor(function(d) { return 2;})
		.maxBubbleRelativeSize(0.05)
		.title(function(d) { return d.key[2];})
		.y(d3.scale.linear().domain([0,16]))
		.x(d3.scale.linear().domain([0.5,1.5]))
		.on('renderlet', function(chart) {

            chart.selectAll("circle").on("click", function (d) {
                chart.filter(null)

                    .filter(d.key)
                    .redrawGroup();
            })
        })
		;


	var composite = dc.compositeChart("#raw-readings");
	composite
	    .width(780)
	    .height(200)
	    .margins({top:10,bottom:60,right:40,left:60})
	    .x(d3.scale.linear().domain([0,20]))
	    .elasticY(true)
	    .elasticX(true)
	    .legend(dc.legend().x(70).y(90).itemHeight(10).gap(5))
	    .compose([
	        dc.lineChart(composite)
	            .dimension(timesecondsDim)
	            .colors('red')
	            .group(remove_empty_bins(reading_x_Group), "X"),
	        dc.lineChart(composite)
	            .dimension(timesecondsDim)
	            .colors('blue')
	            .group(remove_empty_bins(reading_y_Group), "Y"),
			dc.lineChart(composite)
	            .dimension(timesecondsDim)
	            .colors('green')
	            .group(remove_empty_bins(reading_z_Group), "Z")

	        ])
	    .brushOn(false)
	    .mouseZoomable(true)
	    ;


	var composite = dc.compositeChart("#resultant-readings");
	composite
	    .width(780)
	    .height(200)
	    .margins({top:10,bottom:60,right:40,left:60})
	    .x(d3.scale.linear().domain([0,20]))
	    .elasticY(true)
	    .elasticX(true)
	    .compose([
	        dc.lineChart(composite)
	            .dimension(timesecondsDim)
	            .colors('steelblue')
	            .group(remove_empty_bins(reading_xyz_Group))
	        

	        ])
	    .brushOn(false)
	    .mouseZoomable(true)
	    ;	    

    dc.renderAll();
};