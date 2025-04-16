package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var (
	host                     = os.Getenv("HOST")
	port                     = os.Getenv("PORT")
	user                     = os.Getenv("USER")
	password                 = os.Getenv("PASSWORD")
	dbname                   = os.Getenv("DBNAME")
	order_count              = 0
	initial_order_count_k, _ = strconv.Atoi(os.Getenv("INITIAL_ORDER_COUNT_K"))
	order_update_count_k, _  = strconv.Atoi(os.Getenv("ORDER_UPDATE_COUNT_K"))
	new_order_count_k, _     = strconv.Atoi(os.Getenv("NEW_ORDER_COUNT_K"))

	order_statuses = []string{"Created", "Processing", "Shipped", "Delivered", "Cancelled"}
	stores         = []StoreData{}
)

type OrderData struct {
	OrderAmount   float32 `json:"order_amount"`
	OrderStatusId int     `json:"order_status_id"`
	OrderStatus   string  `json:"order_status"`
}

type Loc struct {
	Long float32 `json:"store_long"`
	Lat  float32 `json:"store_lat"`
}

type StoreData struct {
	StoreId   int    `json:"store_id"`
	StoreAddr string `json:"store_addr"`
	StoreLoc  Loc    `json:"store_loc"`
}

type Data struct {
	Order OrderData `json:"order"`
	Store StoreData `json:"store"`
}

func main() {
	fmt.Println("INITIAL_ORDER_COUNT_K: ", initial_order_count_k)
	fmt.Println("ORDER_UPDATE_COUNT_K: ", order_update_count_k)
	fmt.Println("NEW_ORDER_COUNT_K: ", new_order_count_k)

	start := time.Now()
	generateStoreArray()
	generateOrders(initial_order_count_k)
	fmt.Println("Finished generating initial orders in", time.Since(start).Seconds(), "seconds.")
	order_count = initial_order_count_k

	for {
		start = time.Now()
		updateOrders()
		fmt.Println("Finished updating orders in", time.Since(start).Seconds(), "seconds.")
		time.Sleep(250 * time.Millisecond)

		start = time.Now()
		generateOrders(new_order_count_k)
		fmt.Println("Finished generating new orders in", time.Since(start).Seconds(), "seconds.")
		order_count += new_order_count_k

	}
}

func generateOrders(order_count int) {
	psqlInfo := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		host, port, dbname, user, password)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(500)
	db.SetMaxIdleConns(500)

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	store_count := len(stores)

	// generate orders concurrently
	var wg sync.WaitGroup
	for i := 0; i < order_count; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {

				store := stores[rand.Intn(store_count)]

				order := OrderData{float32(rand.Intn(18001)+2000) / 100, 1, "Created"}
				data, _ := json.Marshal(Data{order, store})
				query := `INSERT INTO order_schema.order (data) VALUES ($1);`
				_, err = db.Exec(query, data)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func updateOrders() {
	psqlInfo := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		host, port, dbname, user, password)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// get a random int between 100 and 300 to cancel orders
	cancel_order_count := rand.Intn(300-100) + 100

	// cancel orders
	_, err = db.Exec(`UPDATE order_schema.order
                      SET data = jsonb_set(jsonb_set("data", '{order, order_status_id}', to_jsonb(5)), '{order, order_status}', '"Cancelled"'), updated_at = CURRENT_TIMESTAMP
                      WHERE order_id IN (SELECT order_id
                                         FROM order_schema.order
                                         WHERE data->'order'->'order_status_id' NOT IN (to_jsonb(4),to_jsonb(5))
                                         ORDER BY RANDOM()
                                         LIMIT $1);`, cancel_order_count)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("orders have been cancelled")
	}
	// update remaining orders
	// get list of order_ids to update
	rows, err := db.Query(`SELECT order_id, data->'order'->'order_status_id'
                                FROM order_schema.order
                                WHERE data->'order'->'order_status_id' NOT IN (to_jsonb(4),to_jsonb(5))
                                ORDER BY RANDOM()
                                LIMIT $1;`, order_update_count_k*1000-cancel_order_count)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("order status data retrieved from db")
	}
	order_ids := []int{}
	statuses := []int{}
	var id int
	var status int
	for rows.Next() {
		rows.Scan(&id, &status)
		order_ids = append(order_ids, id)
		statuses = append(statuses, status+1)
	}

	for i := range order_ids {
		sql := fmt.Sprintf(`UPDATE order_schema.order
                      SET data = jsonb_set(jsonb_set("data", '{order, order_status_id}', to_jsonb(%v)), '{order, order_status}', '"%v"'),
                          updated_at = CURRENT_TIMESTAMP
                      WHERE order_id = %v;`, statuses[i], order_statuses[statuses[i]-1], order_ids[i])
		_, err = db.Exec(sql)
		if err != nil {
			panic(err)
		}
	}
}

func generateStoreArray() {
	stores = append(stores, StoreData{1, "", Loc{-72.8073, 40.8736}})
	stores = append(stores, StoreData{2, "", Loc{-94.354748, 38.255521}})
	stores = append(stores, StoreData{3, "", Loc{-80.619022, 27.896343}})
	stores = append(stores, StoreData{4, "", Loc{-87.6394021, 41.4806452}})
	stores = append(stores, StoreData{5, "", Loc{-83.3887, 33.2954}})
	stores = append(stores, StoreData{6, "", Loc{-97.2608, 32.8782}})
	stores = append(stores, StoreData{7, "", Loc{-81.668727, 28.320863}})
	stores = append(stores, StoreData{8, "", Loc{-80.619022, 27.896343}})
	stores = append(stores, StoreData{9, "", Loc{-80.619022, 27.896343}})
	stores = append(stores, StoreData{10, "", Loc{-83.7412, 42.2756}})
	stores = append(stores, StoreData{11, "", Loc{-88.819067, 42.240347}})
	stores = append(stores, StoreData{12, "", Loc{-96.1045, 41.18}})
	stores = append(stores, StoreData{13, "", Loc{-88.1338, 43.1926}})
	stores = append(stores, StoreData{14, "", Loc{-119.123026, 46.241469}})
	stores = append(stores, StoreData{15, "", Loc{-91.1566, 38.2225}})
	stores = append(stores, StoreData{16, "", Loc{-98.3828, 29.5698}})
	stores = append(stores, StoreData{17, "", Loc{-90.5552, 38.593}})
	stores = append(stores, StoreData{18, "", Loc{-81.4061, 40.8954}})
	stores = append(stores, StoreData{19, "", Loc{-86.9139, 33.3322}})
	stores = append(stores, StoreData{20, "", Loc{-118.345, 34.0458}})
	stores = append(stores, StoreData{21, "", Loc{-96.5855, 33.7622}})
	stores = append(stores, StoreData{22, "", Loc{-101.398, 35.6646}})
	stores = append(stores, StoreData{23, "", Loc{-72.9006, 41.8283}})
	stores = append(stores, StoreData{24, "", Loc{-83.899688, 43.414294}})
	stores = append(stores, StoreData{25, "", Loc{-74.7387, 41.072}})
	stores = append(stores, StoreData{26, "", Loc{-81.8722, 26.6249}})
	stores = append(stores, StoreData{27, "", Loc{-106.044, 35.6356}})
	stores = append(stores, StoreData{28, "", Loc{-96.2871, 30.5863}})
	stores = append(stores, StoreData{29, "", Loc{-121.356, 38.7661}})
	stores = append(stores, StoreData{30, "", Loc{-70.8921, 43.1665}})
	stores = append(stores, StoreData{31, "", Loc{-81.723, 28.5923}})
	stores = append(stores, StoreData{32, "", Loc{-95.2626, 29.8503}})
	stores = append(stores, StoreData{33, "", Loc{-74.07, 41.5212}})
	stores = append(stores, StoreData{34, "", Loc{-86.1591, 39.5505}})
	stores = append(stores, StoreData{35, "", Loc{-74.0474, 40.7493}})
	stores = append(stores, StoreData{36, "", Loc{-74.1803, 40.8329}})
	stores = append(stores, StoreData{37, "", Loc{-117.7983, 33.6945}})
	stores = append(stores, StoreData{38, "", Loc{-84.4564, 39.1597}})
	stores = append(stores, StoreData{39, "", Loc{-79.548467, 40.811899}})
	stores = append(stores, StoreData{40, "", Loc{-80.3799, 25.5918}})
	stores = append(stores, StoreData{41, "", Loc{-82.5675, 28.5191}})
	stores = append(stores, StoreData{42, "", Loc{-80.31303, 26.048334}})
	stores = append(stores, StoreData{43, "", Loc{-80.369282, 25.811498}})
	stores = append(stores, StoreData{44, "", Loc{-95.4857, 29.8031}})
	stores = append(stores, StoreData{45, "", Loc{-83.9478, 43.6605}})
	stores = append(stores, StoreData{46, "", Loc{-122.003407, 39.206832}})
	stores = append(stores, StoreData{47, "", Loc{-81.9332, 34.9679}})
	stores = append(stores, StoreData{48, "", Loc{-81.1897, 28.927}})
	stores = append(stores, StoreData{49, "", Loc{-75.1834, 39.9549}})
	stores = append(stores, StoreData{50, "", Loc{-86.7498, 33.1451}})
	stores = append(stores, StoreData{51, "", Loc{-117.933, 33.9302}})
	stores = append(stores, StoreData{52, "", Loc{-72.6837, 41.5539}})
	stores = append(stores, StoreData{53, "", Loc{-116.214, 33.7002}})
	stores = append(stores, StoreData{54, "", Loc{-122.517, 47.171}})
	stores = append(stores, StoreData{55, "", Loc{-117.837022, 33.855303}})
	stores = append(stores, StoreData{56, "", Loc{-90.5557, 38.5932}})
	stores = append(stores, StoreData{57, "", Loc{-90.243397, 38.593772}})
	stores = append(stores, StoreData{58, "", Loc{-74.1406, 40.0533}})
	stores = append(stores, StoreData{59, "", Loc{-83.2611, 30.9127}})
	stores = append(stores, StoreData{60, "", Loc{-96.8416, 33.0263}})
	stores = append(stores, StoreData{61, "", Loc{-112.204, 33.5816}})
	stores = append(stores, StoreData{62, "", Loc{-93.8222, 35.5123}})
	stores = append(stores, StoreData{63, "", Loc{-98.0095, 43.6898}})
	stores = append(stores, StoreData{64, "", Loc{-90.048, 38.6115}})
	stores = append(stores, StoreData{65, "", Loc{-112.165418, 33.610703}})
	stores = append(stores, StoreData{66, "", Loc{-86.5901, 34.7618}})
	stores = append(stores, StoreData{67, "", Loc{-122.958, 45.5048}})
	stores = append(stores, StoreData{68, "", Loc{-118.143806, 33.934598}})
	stores = append(stores, StoreData{69, "", Loc{-97.6105, 35.2914}})
	stores = append(stores, StoreData{70, "", Loc{-112.192, 35.2574}})
	stores = append(stores, StoreData{71, "", Loc{-89.7249, 36.2287}})
	stores = append(stores, StoreData{72, "", Loc{-95.4861, 30.4224}})
	stores = append(stores, StoreData{73, "", Loc{-84.5883, 43.2918}})
	stores = append(stores, StoreData{74, "", Loc{-122.216, 37.4722}})
	stores = append(stores, StoreData{75, "", Loc{-82.5737, 28.0689}})
	stores = append(stores, StoreData{76, "", Loc{-81.151, 28.54}})
	stores = append(stores, StoreData{77, "", Loc{-117.915, 33.6166}})
	stores = append(stores, StoreData{78, "", Loc{-88.0756, 38.7313}})
	stores = append(stores, StoreData{79, "", Loc{-77.053924, 39.039506}})
	stores = append(stores, StoreData{80, "", Loc{-83.621331, 42.254126}})
	stores = append(stores, StoreData{81, "", Loc{-88.0746, 41.8594}})
	stores = append(stores, StoreData{82, "", Loc{-91.8887, 39.1733}})
	stores = append(stores, StoreData{83, "", Loc{-77.9763, 34.7403}})
	stores = append(stores, StoreData{84, "", Loc{-97.7278, 30.3934}})
	stores = append(stores, StoreData{85, "", Loc{-97.7714, 30.4879}})
	stores = append(stores, StoreData{86, "", Loc{-90.3063, 38.8056}})
	stores = append(stores, StoreData{87, "", Loc{-96.9678, 33.0108}})
	stores = append(stores, StoreData{88, "", Loc{-112.16813, 33.37754}})
	stores = append(stores, StoreData{89, "", Loc{-82.3183, 27.8938}})
	stores = append(stores, StoreData{90, "", Loc{-94.0009, 44.146}})
	stores = append(stores, StoreData{91, "", Loc{-90.3021, 38.6185}})
	stores = append(stores, StoreData{92, "", Loc{-83.0815, 36.2524}})
	stores = append(stores, StoreData{93, "", Loc{-90.5123, 38.7876}})
	stores = append(stores, StoreData{94, "", Loc{-88.336569, 42.234597}})
	stores = append(stores, StoreData{95, "", Loc{-90.3808, 38.4995}})
	stores = append(stores, StoreData{96, "", Loc{-90.3183, 38.5836}})
	stores = append(stores, StoreData{97, "", Loc{-86.889, 33.5564}})
	stores = append(stores, StoreData{98, "", Loc{-93.8104, 32.3957}})
	stores = append(stores, StoreData{99, "", Loc{-80.6363, 41.0242}})
	stores = append(stores, StoreData{100, "", Loc{-83.8661, 35.9216}})
	stores = append(stores, StoreData{101, "", Loc{-88.0208, 41.598}})
	stores = append(stores, StoreData{102, "", Loc{-90.399, 38.2881}})
	stores = append(stores, StoreData{103, "", Loc{-78.3489, 34.9914}})
	stores = append(stores, StoreData{104, "", Loc{-111.858619, 33.260085}})
	stores = append(stores, StoreData{105, "", Loc{-97.1069, 31.6055}})
	stores = append(stores, StoreData{106, "", Loc{-86.1162, 39.9619}})
	stores = append(stores, StoreData{107, "", Loc{-89.3959, 36.0647}})
	stores = append(stores, StoreData{108, "", Loc{-82.7283, 27.8146}})
	stores = append(stores, StoreData{109, "", Loc{-122.065, 37.9133}})
	stores = append(stores, StoreData{110, "", Loc{-122.065915, 38.258328}})
	stores = append(stores, StoreData{111, "", Loc{-88.340754, 41.978442}})
	stores = append(stores, StoreData{112, "", Loc{-78.858221, 42.981224}})
	stores = append(stores, StoreData{113, "", Loc{-95.307, 32.2724}})
	stores = append(stores, StoreData{114, "", Loc{-82.5181, 36.5475}})
	stores = append(stores, StoreData{115, "", Loc{-90.9672, 38.9872}})
	stores = append(stores, StoreData{116, "", Loc{-122.334, 47.4672}})
	stores = append(stores, StoreData{117, "", Loc{-92.7434, 38.9383}})
	stores = append(stores, StoreData{118, "", Loc{-70.9426, 42.5481}})
	stores = append(stores, StoreData{119, "", Loc{-96.2635, 30.5552}})
	stores = append(stores, StoreData{120, "", Loc{-89.9519, 38.5171}})
	stores = append(stores, StoreData{121, "", Loc{-78.7174, 34.3059}})
	stores = append(stores, StoreData{122, "", Loc{-122.006, 37.5229}})
	stores = append(stores, StoreData{123, "", Loc{-76.7259, 39.2077}})
	stores = append(stores, StoreData{124, "", Loc{-73.0072, 41.5677}})
	stores = append(stores, StoreData{125, "", Loc{-81.3438, 28.756}})
	stores = append(stores, StoreData{126, "", Loc{-95.3815, 29.7759}})
	stores = append(stores, StoreData{127, "", Loc{-96.7168, 32.8787}})
	stores = append(stores, StoreData{128, "", Loc{-122.193, 48.1519}})
	stores = append(stores, StoreData{129, "", Loc{-90.2439, 38.5937}})
	stores = append(stores, StoreData{130, "", Loc{-102.394, 31.8541}})
	stores = append(stores, StoreData{131, "", Loc{-122.837, 47.0399}})
	stores = append(stores, StoreData{132, "", Loc{-82.9486, 42.6275}})
	stores = append(stores, StoreData{133, "", Loc{-77.8998, 34.126}})
	stores = append(stores, StoreData{134, "", Loc{-117.25603, 33.04596}})
	stores = append(stores, StoreData{135, "", Loc{-92.3953, 32.7704}})
	stores = append(stores, StoreData{136, "", Loc{-121.84, 38.0061}})
	stores = append(stores, StoreData{137, "", Loc{-96.8894, 32.9539}})
	stores = append(stores, StoreData{138, "", Loc{-89.1512, 38.5293}})
	stores = append(stores, StoreData{139, "", Loc{-114.617308, 32.669388}})
	stores = append(stores, StoreData{140, "", Loc{-116.213, 33.739}})
	stores = append(stores, StoreData{141, "", Loc{-71.492223, 43.223129}})
	stores = append(stores, StoreData{142, "", Loc{-94.3505, 37.838}})
	stores = append(stores, StoreData{143, "", Loc{-84.215, 33.7169}})
	stores = append(stores, StoreData{144, "", Loc{-84.9197, 32.4653}})
	stores = append(stores, StoreData{145, "", Loc{-79.3123, 42.4572}})
	stores = append(stores, StoreData{146, "", Loc{-122.181, 48.1002}})
	stores = append(stores, StoreData{147, "", Loc{-121.838, 38.0061}})
	stores = append(stores, StoreData{148, "", Loc{-90.872, 38.8119}})
	stores = append(stores, StoreData{149, "", Loc{-96.878817, 32.720184}})
	stores = append(stores, StoreData{150, "", Loc{-86.8757, 33.4976}})
	stores = append(stores, StoreData{151, "", Loc{-71.8712, 42.179}})
	stores = append(stores, StoreData{152, "", Loc{-75.8895, 42.1046}})
	stores = append(stores, StoreData{153, "", Loc{-121.943, 37.2683}})
	stores = append(stores, StoreData{154, "", Loc{-81.3901, 28.6085}})
	stores = append(stores, StoreData{155, "", Loc{-93.7968, 41.0254}})
	stores = append(stores, StoreData{156, "", Loc{-91.1422, 38.8169}})
	stores = append(stores, StoreData{157, "", Loc{-90.1433, 38.7026}})
	stores = append(stores, StoreData{158, "", Loc{-82.771, 36.1826}})
	stores = append(stores, StoreData{159, "", Loc{-90.1802, 38.9229}})
	stores = append(stores, StoreData{160, "", Loc{-97.1791, 33.3658}})
	stores = append(stores, StoreData{161, "", Loc{-122.714, 38.3649}})
	stores = append(stores, StoreData{162, "", Loc{-86.579, 34.7196}})
	stores = append(stores, StoreData{163, "", Loc{-77.0528, 39.0381}})
	stores = append(stores, StoreData{164, "", Loc{-86.0556, 39.7714}})
	stores = append(stores, StoreData{165, "", Loc{-89.9856, 38.5956}})
	stores = append(stores, StoreData{166, "", Loc{-94.4094, 33.4722}})
	stores = append(stores, StoreData{167, "", Loc{-90.7005, 38.8041}})
	stores = append(stores, StoreData{168, "", Loc{-122.331, 37.9549}})
	stores = append(stores, StoreData{169, "", Loc{-97.7412, 31.1248}})
	stores = append(stores, StoreData{170, "", Loc{-90.516, 38.8204}})
	stores = append(stores, StoreData{171, "", Loc{-90.1216, 38.8835}})
	stores = append(stores, StoreData{172, "", Loc{-90.0679, 38.8669}})
	stores = append(stores, StoreData{173, "", Loc{-95.1685, 29.8076}})
	stores = append(stores, StoreData{174, "", Loc{-82.2591, 34.9417}})
	stores = append(stores, StoreData{175, "", Loc{-87.78851119, 41.90965166}})
	stores = append(stores, StoreData{176, "", Loc{-82.7986, 42.7183}})
	stores = append(stores, StoreData{177, "", Loc{-90.2634, 38.6508}})
	stores = append(stores, StoreData{178, "", Loc{-123.804, 46.9774}})
	stores = append(stores, StoreData{179, "", Loc{-97.8452, 30.5597}})
	stores = append(stores, StoreData{180, "", Loc{-96.761753, 32.875531}})
	stores = append(stores, StoreData{181, "", Loc{-84.1654, 36.7255}})
	stores = append(stores, StoreData{182, "", Loc{-96.7971, 33.0958}})
	stores = append(stores, StoreData{183, "", Loc{-73.815463, 42.713186}})
	stores = append(stores, StoreData{184, "", Loc{-86.9943, 33.3338}})
	stores = append(stores, StoreData{185, "", Loc{-90.3183, 38.5836}})
	stores = append(stores, StoreData{186, "", Loc{-72.5512, 42.3582}})
	stores = append(stores, StoreData{187, "", Loc{-97.4252, 35.8586}})
	stores = append(stores, StoreData{188, "", Loc{-119.775, 36.8494}})
	stores = append(stores, StoreData{189, "", Loc{-71.169384, 42.267443}})
	stores = append(stores, StoreData{190, "", Loc{-93.3613, 43.1459}})
	stores = append(stores, StoreData{191, "", Loc{-74.4032, 40.8643}})
	stores = append(stores, StoreData{192, "", Loc{-121.996, 37.2925}})
	stores = append(stores, StoreData{193, "", Loc{-80.1853, 32.9967}})
	stores = append(stores, StoreData{194, "", Loc{-90.3724, 38.4466}})
	stores = append(stores, StoreData{195, "", Loc{-80.6086, 28.3518}})
	stores = append(stores, StoreData{196, "", Loc{-93.6591, 32.4563}})
	stores = append(stores, StoreData{197, "", Loc{-75.5583, 38.0753}})
	stores = append(stores, StoreData{198, "", Loc{-73.9714, 40.8596}})
	stores = append(stores, StoreData{199, "", Loc{-82.1214, 27.9882}})
	stores = append(stores, StoreData{200, "", Loc{-96.392235, 35.839521}})
	stores = append(stores, StoreData{201, "", Loc{-97.0806, 33.0671}})
	stores = append(stores, StoreData{202, "", Loc{-104.6611, 38.31649}})
	stores = append(stores, StoreData{203, "", Loc{-90.3804, 38.5001}})
	stores = append(stores, StoreData{204, "", Loc{-77.8789, 34.2276}})
	stores = append(stores, StoreData{205, "", Loc{-122.299, 38.3184}})
	stores = append(stores, StoreData{206, "", Loc{-94.1015, 40.2517}})
	stores = append(stores, StoreData{207, "", Loc{-76.8189, 42.0567}})
}
