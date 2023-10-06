const fs = require('fs')


const fileLocation = 'annual-enterprise-survey-2021-financial-year-provisional-csv.csv'


const extractCsv = ()=>{
    const stream = fs.createReadStream(fileLocation)

    let data = ''
    return new Promise((resolve,reject)=>{
        stream.on("data",(chunk)=>{
            data += chunk
        })

        stream.on("end",()=>{
           resolve(data)
        })
    })

}

const getRows = (raw)=>{
    return raw.split("\r\n")
}


const getMappedData = (rows)=>{

    if(!Array.isArray(rows)) return []
    
    const map = []

    // Split the first row in rows above into an array. 
    // To be used later as keys for all objects
    const headers = rows[0].split(',')

    // Loop through the array of rows and map the key to the data in
    for(let i=1; i < rows.length - 1; i++){


        const arr = rows[i].split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/)
        const obj = {}

        // Map data to key and attach obj variable above
        for(let j =0; j < arr.length; j++){
            if(headers[j] === "Value"){
                let value = 0
                if(!isNaN(arr[j])){
                    value = Number(arr[j])
                }
                obj[headers[j]] = value
            }else{
                obj[headers[j]] = arr[j]
            }
        } 

        map.push(obj)
    }

    return map
}


const groupByIndustryName = (arr)=>{
    return arr.reduce((group, element) => {
        const { Industry_name_NZSIOC } = element;
        group[Industry_name_NZSIOC] = group[Industry_name_NZSIOC] ?? [];
        group[Industry_name_NZSIOC].push(element);
        return group;
      }, {});
}


const getValueSummary = async()=>{

    const data = await extractCsv()

    const rows = getRows(data)

    
    const surveys = getMappedData(rows)
   
    
    const groupedByIndustryName = groupByIndustryName(surveys)
    
      const summaryOfValueByIndustryName = {}

      for(prop in groupedByIndustryName){
        const sum = groupedByIndustryName[prop].reduce((acc, curr)=>{
            return acc += parseFloat(curr.Value)
        },0)

        summaryOfValueByIndustryName[prop] = sum
      }

      return summaryOfValueByIndustryName
}



(async()=>{
    const result = await getValueSummary()

    console.log(result)

})()