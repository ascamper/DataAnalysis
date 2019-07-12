<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
<style>
</style>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          ['YEARWEEK', 'TIME_SERIES'],
          ${qty_data}
        ]);

        var options = {
          title: "TIME_SERIES",
          curveType: 'function',
          legend: { position: 'bottom' }
        };

        var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));

        chart.draw(data, options);
      }
    </script>
<link href="${pageContext.request.contextPath}/resources/TableCss.css"
	rel="stylesheet" />
</head>

<body>
	<div id="nav">
		<form class="input" method="get" action="filter_data_table">
			<select name="seg1">
				<option value="A01" selected>A01</option>
			</select> <select name="seg2">
				<option value="SITEID0001" selected>SITEID0001</option>
				<option value="SITEID0002">SITEID0002</option>
				<option value="SITEID0003">SITEID0003</option>
				<option value="SITEID0004">SITEID0004</option>
				<option value="SITEID0005">SITEID0005</option>
				<option value="SITEID0006">SITEID0006</option>
			</select> <select name="product_group">
				<option value="PG01" selected>PG01</option>
				<option value="PG02">PG02</option>
				<option value="PG03">PG03</option>
				<option value="PG04">PG04</option>
				<option value="PG05">PG05</option>
			</select> <select name="item">
				${item_list}
			</select> <input class="run" type="submit" value="적용" />
		</form>
		<form action="/hyun/table" method="get">
			<input type="submit" value="Original" name="mode" />
		</form>
		<form action="/hyun/table" method="get">
			<input type="submit" value="model" name="mode" />
		</form>
		<a href="/hyun"> <input type="button" value="뒤로" name="main" />
		</a>
	</div>

	<div>
		<table>
			<tr>
				<th>REGIONSEG</th>
				<th>REGIONSEG3</th>
				<th>SALESID</th>
				<th>PRODUCTGROUP</th>
				<th>ITEM</th>
				<th>YEARWEEK</th>
				<th>MAP_PRICE</th>
				<th>IR</th>
				<th>PMAP</th>
				<th>PMAP10</th>
				<th>PRO_PERCENT</th>
				<th>QTY</th>
				<th>PROMOTIONNY</th>
				<th>MOVING_AVG</th>
				<th>SEASONALITY</th>
				<th>FCST</th>
				<th>TIME_SERIES</th>
			</tr>
			${select_result}
		</table>
	</div>
	<div id="curve_chart" style="width: 1800; height: 500px"></div>
</body>
</html>