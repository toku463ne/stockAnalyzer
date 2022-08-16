const axios = require('axios');

const data = {
    instrument: 'MSFT',
    granularity: 'D',
    start: '2021-11-01T00:00:00',
    end: '2021-12-01T00:00:00'
};

axios.post('http://localhost:9000/price', data)
    .then((res) => {
        console.log("post done")
        console.log(`Status: ${res.status}`);
        console.log('Body: ', res.data);
        //return res.data;
    }).catch((err) => {
        console.error(err);
    });

