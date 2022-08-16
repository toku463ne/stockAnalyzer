

exports.showChart = function(req, res) {
    chart_params = JSON.parse(req.body.chartparams)
    let s = ""
    for (let key in chart_params) {
        if (s != "") {
            s += "&"
        }
        s += key + "=" + chart_params[key]
    }
    console.log(s)
    //res.render("chart", { "title": "chart", 
    //    "chart_params": s});
}