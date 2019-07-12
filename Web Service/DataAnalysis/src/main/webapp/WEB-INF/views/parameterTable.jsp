<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<html>
<head>
<title>Parameter Table</title>
<link href="${pageContext.request.contextPath}/resources/PageCss.css"
	rel="stylesheet" />
</head>
<body>
	<div class="form">
		<span>
			<h1>Parameter Table</h1>
			<form action="/hyun/analysing" method="get">
				<input type="submit" value="Run analysis model" name="run" class="loginBtn" />
			</form>
		</span>
		<span>
			<table>
				<tr>
					<th>Parameter</th>
					<th>value</th>
					<th>checkbox</th>
				</tr>
				${table}
			</table>
			<a href="/hyun/table">
				<input type="button" value="Go table" name="table" class="loginBtn" />
			</a>
		</span>
		<span>
			<form action="refact" method="get">
				Parameter: <select name="parameter" class="txtb"> ${parameter}
				</select><br>
				<input type="number" min="3" max="20" name="value" class="txtb" placeholder="value"/>
				<input type="submit" value="Add Parameter" name="button" class="loginBtn" />
				<input type="submit" value="Modify" name="button" class="loginBtn" />
				<input type="submit" value="delete" name="button" class="loginBtn" />
			</form>
		</span>
	</div>

</body>
</html>
