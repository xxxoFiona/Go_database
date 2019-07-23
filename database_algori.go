package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-gomail/gomail"
	_ "github.com/go-sql-driver/mysql"

	//"github.com/robfig/cron"

	"github.com/tealeg/xlsx"
)

var (
	tables            = make([]string, 0) //Create dynamic string
	dataBaseTable     = ""                //create dynamic string
	feedtime      int = 1                 //as long as data exist in database, assume the original feedtime is 1
)

const helpInfo = `port:3306;
`

type xlsl_info struct {
	feed_count       []int
	pigId            []string
	feed_time        []string
	scale_data       []float64
	feed_starttime   []string
	feed_endtime     []string
	feedduration     []string
	feed_weight      []float64
	feeding_duration []string
	totalfeed        float64
	totalupload      float64
	totaltime        string
}

type feed_superviseinfo struct {
	advise_feedweight   float32
	actually_feedweight float32
	actually_eatweight  float32
}

type daily_info struct {
	pig_number     int
	pig_age        int
	advise_weight  float32
	time_supervise bool
	feed_supervise []feed_superviseinfo
}

//original information about sql
func init() {
	port := flag.Int("port", 3306, "default port is:")
	addr := flag.String("addr", "192.168.100.102", "default address is :127.0.0.1")
	user := flag.String("user", "root", "default user is:root")
	pwd := flag.String("pwd", "", "dfault password is:0pl,9okm")
	db := flag.String("db", "fodder", "default databse name is:fodder")
	tabs := flag.String("tabs", "", "default tables name is:fodder_log")
	flag.Usage = usage
	flag.Parse()
	tables = append(tables, strings.Split(*tabs, ",")...)                                           //tables = feed_log
	dataBaseTable = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", *user, *pwd, *addr, *port, *db) //sql's information
}

//log in to database and select data, then save them as csvfile
func querySQL(db *sql.DB, table string, ch chan bool) {
	fmt.Println("开始：", table)
	d := time.Now()
	d2 := d.AddDate(0, 0, -3)
	dlast := d2.Format("2006-01-02 00:00:00")
	d1 := d.AddDate(0, 0, -2)
	dnow := d1.Format("2006-01-02 00:00:00")
	//select today's data from dataset, you can change date to meet your needs
	rows, err := db.Query(fmt.Sprintf("select * from %s where feed_time>='"+dlast+"' and feed_time <='"+dnow+"' and device_id='3746a505152c60aa'", table))
	if err != nil {
		panic(err)
	}
	columns, err := rows.Columns() //columns:[sty_id fodder_id device_id feed_time current_weight feed_weight advise_value cmd_type log_time]
	fmt.Println(columns)
	if err != nil {
		panic(err.Error())
	}
	//get the column of rows and associate the value of the column name parameter with the column address
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i] //pointer scanArgs point to values
	}
	totalValues := make([][]string, 0)
	//line by line storage the data
	for rows.Next() {
		var s []string
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		for _, v := range values {
			s = append(s, string(v))
		}
		totalValues = append(totalValues, s)
	}
	if err = rows.Err(); err != nil {
		panic(err.Error())
	}
	writeToCSV(table+".csv", columns, totalValues)
	ch <- true
}

func writeToCSV(file string, columns []string, totalValues [][]string) {
	f, err := os.Create(file)
	defer f.Close()
	if err != nil {
		panic(err)
	}
	w := csv.NewWriter(f)
	for i, row := range totalValues {
		if i == 0 {
			w.Write(columns) //write [sty_id fodder_id device_id feed_time current_weight feed_weight advise_value cmd_type log_time] as first line
			w.Write(row)
		} else {
			w.Write(row)
		}
	}
	w.Flush()
	fmt.Println("结束：", file)
}

func usage() {
	fmt.Fprint(os.Stderr, helpInfo)
}

func senderrmail() {
	today := time.Now().Format("01-02")
	m := gomail.NewMessage()
	m.SetAddressHeader("From", "wanqiuw97@163.com", "成都联珠科技有限公司")
	//m.SetHeader("To", m.FormatAddress("13880212581@163.com", "收件人"), m.FormatAddress("18188335154@163.com", "收件人"), m.FormatAddress("18810565071@163.com", "收件人"))
	m.SetHeader("To", "18940855382@163.com")
	m.SetHeader("Subject", fmt.Sprintf("%v数据库无数据", today)) //title
	m.SetHeader("Cc", m.FormatAddress("wanqiuw97@163.com", "抄送"))
	d := gomail.NewPlainDialer("smtp.163.com", 25, "wanqiuw97@163.com", "wwq12345") // sending mail server, port, sender account, sender authorization password
	if err := d.DialAndSend(m); err != nil {
		log.Println("发送失败", err)
		sendmail()
	}
	log.Println("done.发送成功")
}

func sendmail() {
	today := time.Now().Format("01-02")
	m := gomail.NewMessage()
	m.SetAddressHeader("From", "wanqiuw97@163.com", "成都联珠科技有限公司")
	//m.SetHeader("To", m.FormatAddress("13880212581@163.com", "收件人"), m.FormatAddress("18188335154@163.com", "收件人"), m.FormatAddress("18810565071@163.com", "收件人"))
	m.SetHeader("To", "18940855382@163.com")
	m.SetHeader("Subject", fmt.Sprintf("%v养殖户生产数据报表", today))
	m.SetHeader("Cc", m.FormatAddress("wanqiuw97@163.com", "抄送"))
	bytes := today + "养殖户生产数据报表.xlsx"
	m.Attach(bytes)
	m.Attach("feed_log.csv")
	d := gomail.NewPlainDialer("smtp.163.com", 25, "wanqiuw97@163.com", "wwq12345")
	if err := d.DialAndSend(m); err != nil {
		log.Println("发送失败", err)
		sendmail()
	}
	log.Println("done.发送成功")
}

//func extract_from_csv aims to extract data from feed_log.csv then save them into struct xlsl_info
func (xlslinfo1 *xlsl_info) extract_from_csv() {
	cuty, err := ioutil.ReadFile("feed_log.csv")
	if err != nil {
		panic(err.Error())
	}
	r1 := csv.NewReader(strings.NewReader(string(cuty)))
	ss, _ := r1.ReadAll()
	sz := len(ss)
	if ss[1][0] == "uploading" {
		xlslinfo1.pigId = append(xlslinfo1.pigId, "上料")
	} else {
		xlslinfo1.pigId = append(xlslinfo1.pigId, ss[1][0])
	}
	xlslinfo1.feed_time = append(xlslinfo1.feed_time, ss[1][3])
	xlslinfo1.feed_starttime = append(xlslinfo1.feed_starttime, ss[1][3])
	v, _ := strconv.ParseFloat(ss[1][4], 32)
	xlslinfo1.scale_data = append(xlslinfo1.scale_data, v)
	xlslinfo1.feed_count = append(xlslinfo1.feed_count, feedtime)
	for i := 2; i < sz-1; i++ {
		tstart, _ := time.Parse("2006-01-02 15:04:05", ss[i][3])
		tend, _ := time.Parse("2006-01-02 15:04:05", ss[i+1][3])
		interval := tend.Sub(tstart)
		if interval.Hours() >= 2 {
			tend1, _ := time.Parse("2006-01-02 15:04:05", ss[i+2][3])
			interval1 := tend1.Sub(tend)
			if interval1.Hours() < 2 {
				if ss[i][0] == "uploading" {
					xlslinfo1.pigId = append(xlslinfo1.pigId, "上料")
				} else {
					xlslinfo1.pigId = append(xlslinfo1.pigId, ss[i][0])
				}
				xlslinfo1.feed_time = append(xlslinfo1.feed_time, ss[i][3])
				xlslinfo1.feed_endtime = append(xlslinfo1.feed_endtime, ss[i][3])
				v0, _ := strconv.ParseFloat(ss[i][4], 32)
				xlslinfo1.scale_data = append(xlslinfo1.scale_data, v0)
				xlslinfo1.feed_count = append(xlslinfo1.feed_count, feedtime)
				if ss[i+1][0] == "uploading" {
					xlslinfo1.pigId = append(xlslinfo1.pigId, "上料")
				} else {
					xlslinfo1.pigId = append(xlslinfo1.pigId, ss[i+1][0])
				}
				xlslinfo1.feed_time = append(xlslinfo1.feed_time, ss[i+1][3])
				xlslinfo1.feed_starttime = append(xlslinfo1.feed_starttime, ss[i+1][3])
				v1, _ := strconv.ParseFloat(ss[i+1][4], 32)
				xlslinfo1.scale_data = append(xlslinfo1.scale_data, v1)
				feedtime++
				xlslinfo1.feed_count = append(xlslinfo1.feed_count, feedtime)
				i++
			} else {
				if ss[i][0] == "uploading" {
					xlslinfo1.pigId = append(xlslinfo1.pigId, "上料")
				} else {
					xlslinfo1.pigId = append(xlslinfo1.pigId, ss[i][0])
				}
				xlslinfo1.feed_time = append(xlslinfo1.feed_time, ss[i][3])
				v0, _ := strconv.ParseFloat(ss[i][4], 32)
				xlslinfo1.scale_data = append(xlslinfo1.scale_data, v0)
				xlslinfo1.feed_count = append(xlslinfo1.feed_count, feedtime)
				i++
			}
		} else if (ss[i][0] != ss[i-1][0]) || (ss[i][0] != ss[i+1][0]) {
			if ss[i][0] == "uploading" {
				xlslinfo1.pigId = append(xlslinfo1.pigId, "上料")
			} else {
				xlslinfo1.pigId = append(xlslinfo1.pigId, ss[i][0])
			}
			xlslinfo1.feed_time = append(xlslinfo1.feed_time, ss[i][3])
			v2, _ := strconv.ParseFloat(ss[i][4], 32)
			xlslinfo1.scale_data = append(xlslinfo1.scale_data, v2)
			xlslinfo1.feed_count = append(xlslinfo1.feed_count, feedtime)
		}
	}
	if ss[sz-1][0] == "uploading" {
		xlslinfo1.pigId = append(xlslinfo1.pigId, "上料")
	} else {
		xlslinfo1.pigId = append(xlslinfo1.pigId, ss[sz-1][0])
	}
	xlslinfo1.feed_time = append(xlslinfo1.feed_time, ss[sz-1][3])
	xlslinfo1.feed_endtime = append(xlslinfo1.feed_endtime, ss[sz-1][3])
	v3, _ := strconv.ParseFloat(ss[sz-1][4], 32)
	xlslinfo1.scale_data = append(xlslinfo1.scale_data, v3)
	xlslinfo1.feed_count = append(xlslinfo1.feed_count, feedtime)
}

func (xlslinfo *xlsl_info) caldata_new() {
	t5 := time.Now()
	t6 := time.Now()
	zerotime := t6.Sub(t5)
	totaltime1, _ := time.Parse("04:05", zerotime.String())
	for i := 1; i < len(xlslinfo.feed_count); i = i + 2 {
		xlslinfo.feed_weight = append(xlslinfo.feed_weight, xlslinfo.scale_data[i-1]-xlslinfo.scale_data[i])
		t1, _ := time.Parse("2006-01-02 15:04:05", xlslinfo.feed_time[i-1])
		t2, _ := time.Parse("2006-01-02 15:04:05", xlslinfo.feed_time[i])
		duartion := t2.Sub(t1)
		totaltime1 = totaltime1.Add(duartion)
		fmt.Println(totaltime1.String())
		xlslinfo.feedduration = append(xlslinfo.feedduration, duartion.String())
	}
	for i := 0; i < len(xlslinfo.feed_starttime); i++ {
		t3, _ := time.Parse("2006-01-02 15:04:05", xlslinfo.feed_starttime[i])
		t4, _ := time.Parse("2006-01-02 15:04:05", xlslinfo.feed_endtime[i])
		feedingduartion := t4.Sub(t3)
		xlslinfo.feeding_duration = append(xlslinfo.feeding_duration, feedingduartion.String())
	}
	time0, _ := time.Parse("2006-01-02 15:04:05", zerotime.String())
	totalduration := totaltime1.Sub(time0)
	xlslinfo.totaltime = totalduration.String()
}

func (xlslinfo1 *xlsl_info) shownin_excel() {
	today := time.Now().Format("01-02")
	xlslinfo1.totalfeed = 0
	xlslinfo1.totalupload = 0
	var row *xlsx.Row
	var cell *xlsx.Cell
	xlfile := xlsx.NewFile()
	sheet, _ := xlfile.AddSheet("Sheet1")
	row = sheet.AddRow()
	row.SetHeightCM(1)
	cell = row.AddCell()
	cell.Value = "农场主"
	cell = row.AddCell()
	cell.Value = "猪栏号"
	cell = row.AddCell()
	cell.Value = "喂料时间"
	cell = row.AddCell()
	cell.Value = "猪只数"
	cell = row.AddCell()
	cell.Value = "持续时间"
	cell = row.AddCell()
	cell.Value = "喂料重量/kg"
	cell = row.AddCell()
	cell.Value = "喂料次数"
	cell = row.AddCell()
	cell.Value = "上料重量/kg"
	for i := 0; i < len(xlslinfo1.feed_count)/2; i++ {
		row = sheet.AddRow()
		cell = row.AddCell()
		cell.Value = "833101号场"
		cell = row.AddCell()
		cell.Value = xlslinfo1.pigId[2*i]
		cell = row.AddCell()
		cell.Value = xlslinfo1.feed_time[2*i]
		cell = row.AddCell()
		if xlslinfo1.pigId[2*i] == "106" {
			cell.Value = "19"
		} else if xlslinfo1.pigId[2*i] == "107" {
			cell.Value = "12"
		} else if xlslinfo1.pigId[2*i] == "108" {
			cell.Value = "4"
		} else if xlslinfo1.pigId[2*i] == "112" {
			cell.Value = "27"
		} else {
			cell.Value = " "
		}
		cell = row.AddCell()
		cell.Value = xlslinfo1.feedduration[i]
		cell = row.AddCell()
		weightnow := xlslinfo1.feed_weight[i] / 1000
		if weightnow > 0 {
			cell.Value = strconv.FormatFloat(weightnow, 'f', -1, 64)
			xlslinfo1.totalfeed = xlslinfo1.totalfeed + weightnow
		}
		cell = row.AddCell()
		cell.Value = fmt.Sprintf("第%d次", xlslinfo1.feed_count[2*i])
		cell = row.AddCell()
		weightnow1 := xlslinfo1.feed_weight[i] / 1000
		if weightnow1 < 0 {
			cell.Value = strconv.FormatFloat(-weightnow1, 'f', -1, 64)
			xlslinfo1.totalupload = xlslinfo1.totalupload - weightnow1
		}
	}
	row = sheet.AddRow()
	cell = row.AddCell()
	cell.Value = "当日合计"
	cell = row.AddCell()
	cell = row.AddCell()
	cell = row.AddCell()
	cell = row.AddCell()
	cell.Value = xlslinfo1.totaltime
	cell = row.AddCell()
	cell.Value = strconv.FormatFloat(xlslinfo1.totalfeed, 'f', -1, 64)
	cell = row.AddCell()
	cell = row.AddCell()
	cell.Value = strconv.FormatFloat(xlslinfo1.totalupload, 'f', -1, 64)
	bytes := today + "养殖户生产数据报表.xlsx"
	xlfile.Save(bytes)
}

func main() {
	//today := time.Now().Format("01-02")
	count := len(tables)
	ch := make(chan bool, count)                //create chan
	db, err := sql.Open("mysql", dataBaseTable) //connect to sql
	defer db.Close()                            //close the database and release all open resources
	if err != nil {
		panic(err.Error())
	}
	err = db.Ping() //Ping() is used to validate the connection after Open
	if err != nil {
		panic(err.Error())
	}

	for _, table := range tables { //using range on the array passes in two variables, index and value. In the example above, we do not need to use the ordinal number of the element, so we omit the blank character "_". Sometimes we do need to know its index
		go querySQL(db, table, ch)

	}
	for i := 0; i < count; i++ {
		<-ch
	} //go routine
	cuty, err := ioutil.ReadFile("feed_log.csv")
	if err != nil {
		panic(err.Error())
	}
	r1 := csv.NewReader(strings.NewReader(string(cuty)))
	ss, _ := r1.ReadAll()
	sz := len(ss)
	//if database is empty,go senderrmail
	if sz == 0 {
		senderrmail()
	} else {
		xlslinfor := new(xlsl_info)
		xlslinfor.extract_from_csv()
		xlslinfor.caldata_new()
		//fmt.Println(xlslinfor)
		xlslinfor.shownin_excel()
		fmt.Println("done")
		sendmail()
	}
}
