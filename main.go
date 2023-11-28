package main

import (
	"fmt"
	"log"
	"time"

	"database/sql"

	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	// "go.starlark.net/lib/time"
)

type Todo struct {
	Id   int    `db:"ID"`
	Name string `db:"NAME"`
}

type SampleData struct {
	DatePeriode            string         `db:"DATEPERIODE"`
	OfficeCode             string         `db:"OFFICE_CODE"`
	CoyID                  string         `db:"COY_ID"`
	RegionalID             string         `db:"REGIONAL_ID"`
	NamaCabang             string         `db:"NAMA_CABANG"`
	CustID                 string         `db:"CUST_ID"`
	ContractNo             string         `db:"CONTRACT_NO"`
	Nama                   string         `db:"NAMA"`
	TanggalLahir           string         `db:"TANGGAL_LAHIR"`
	Umur                   string         `db:"UMUR"`
	AlamatKTP              string         `db:"ALAMAT_KTP"`
	AlamatTagih            string         `db:"ALAMAT_TAGIH"`
	NoFixPhone             string         `db:"NO_FIX_PHONE"`
	NoHandphone            string         `db:"NO_HANDPHONE"`
	AlamatEmergency        string         `db:"ALAMAT_EMERGENCY"`
	NoFixPhoneEmergency    string         `db:"NO_FIX_PHONE_EMERGENCY"`
	NoHPEmergency          string         `db:"NO_HP_EMERGENCY"`
	AlamatOffice           string         `db:"ALAMAT_OFFICE"`
	NoTelpOffice           string         `db:"NO_TELP_OFFICE"`
	NoZipCode              string         `db:"NO_ZIP_CODE"`
	SubZipCode             string         `db:"SUB_ZIP_CODE"`
	NamaKelurahan          string         `db:"NAMA_KELURAHAN"`
	NamaKecamatan          string         `db:"NAMA_KECAMATAN"`
	NamaKabupaten          string         `db:"NAMA_KABUPATEN"`
	NamaProvinsi           string         `db:"NAMA_PROVINSI"`
	Jenis                  int            `db:"JENIS"`
	Top                    string         `db:"TOP"`
	SukuBunga              string         `db:"SUKUBUNGA"`
	TgRealis               string `db:"TG_REALIS"`
	TgJTempoKontrak        string `db:"TG_JTEMPO_KONTRAK"`
	TgAngsur               string `db:"TG_ANGSUR"`
	TgFixAng               string `db:"TG_FIXANG"`
	Pekerjaan              string         `db:"PEKERJAAN"`
	KTP                    string         `db:"KTP"`
	NamaSurveyor           string         `db:"NAMA_SURVEYOR"`
	NamaKolektor           string         `db:"NAMA_KOLEKTOR"`
	NominalGross           string `db:"NOMINAL_GROSS"`
	Admin                  sql.NullString `db:"ADMIN"`
	Asuransi               sql.NullString `db:"ASURANSI"`
	NominalPencairanNett   sql.NullString `db:"NOMINAL_PENCAIRAN_NETT"`
	AngsuranPerBulan       sql.NullString `db:"ANGSURAN_PER_BULAN"`
	AngPokok               sql.NullString `db:"ANG_POKOK"`
	AngBunga               sql.NullString `db:"ANG_BUNGA"`
	ByrPokok               sql.NullString `db:"BYR_POKOK"`
	ByrBunga               sql.NullString `db:"BYR_BUNGA"`
	SldPokok               sql.NullString `db:"SLD_POKOK"`
	SldBunga               sql.NullString `db:"SLD_BUNGA"`
	StatusKontrak          string         `db:"STATUS_KONTRAK"`
	TgLunas                sql.NullString `db:"TGLUNAS"`
	StatusOrder            string         `db:"STATUS_ORDER"`
	JenisObj               string         `db:"JENIS_OBJ"`
	MerkObj                string         `db:"MERK_OBJ"`
	TypeObj                string         `db:"TYPE_OBJ"`
	NoMesin                string         `db:"NO_MESIN"`
	NoRangka               string         `db:"NO_RANGKA"`
	Tahun                  string         `db:"TAHUN"`
	Warna                  string         `db:"WARNA"`
	NoPolisi               string         `db:"NO_POLISI"`
	NoBPKB                 string         `db:"NO_BPKB"`
	NamaBPKB               string         `db:"NAMA_BPKB"`
	HargaPasarKendaraan    sql.NullString `db:"HARGA_PASAR_KENDARAAN"`
	PersentasePlafon       sql.NullString `db:"PERSENTASE_PLAFON"`
	AngsTerakhirBayarKe    int8           `db:"ANGS_TERAKHIR_BAYAR_KE"`
	AngsShrnyaBlnIni       int8           `db:"ANGS_SHRNYA_BLN_INI"`
	JmlAngsTertunggak      int8           `db:"JML_ANGS_TERTUNGGAK"`
	ODHariIni              int8           `db:"OD_HARI_INI"`
	KolHariIni             int8           `db:"KOL_HARI_INI"`
	CycleHariIni           sql.NullString `db:"CYCLE_HARI_INI"`
	OverJtHariIni          sql.NullString `db:"OVER_JT_HARI_INI"`
	StatusNPLHariIni       string         `db:"STATUS_NPL_HARI_INI"`
	StatusCNPLHariIni      string         `db:"STATUS_CNPL_HARI_INI"`
	AMBCDenda              sql.NullString `db:"AMBC_DENDA"`
	AMBCPokok              sql.NullString `db:"AMBC_POKOK"`
	AMBCBunga              sql.NullString `db:"AMBC_BUNGA"`
	TotalKewajiban         sql.NullString `db:"TOTAL_KEWAJIBAN"`
	ACDenda                sql.NullString `db:"AC_DENDA"`
	ACPokok                sql.NullString `db:"AC_POKOK"`
	ACBunga                sql.NullString `db:"AC_BUNGA"`
	TotalAC                sql.NullString `db:"TOTAL_AC"`
	ODAkhirBulan           string         `db:"OD_AKHIR_BULAN"`
	KolAkhirBulan          string         `db:"KOL_AKHIR_BULAN"`
	CycleAkhirBulan        sql.NullString `db:"CYCLE_AKHIR_BULAN"`
	OverJtPerAkhirBln      sql.NullString `db:"OVER_JT_PER_AKHIR_BLN"`
	StsNPLClsngAkhrBln     string         `db:"STS_NPL_CLSNG_AKHR_BLN"`
	StsCNPLClsngAkhrBln    string         `db:"STS_CNPL_CLSNG_AKHR_BLN"`
	ODAwalBulan            string         `db:"OD_AWAL_BULAN"`
	KolAwalBulan           string         `db:"KOL_AWAL_BULAN"`
	CycleAwalBulan         sql.NullString `db:"CYCLE_AWAL_BULAN"`
	OverJtPerAwalBulan     sql.NullString `db:"OVER_JT_PER_AWAL_BULAN"`
	StatusNPLAwalBulan     string         `db:"STATUS_NPL_AWAL_BULAN"`
	StatusCNPLAwalBulan    string         `db:"STATUS_CNPL_AWAL_BULAN"`
	TahunPMK               sql.NullString `db:"TAHUN_PMK"`
	BulanRealisasi         string         `db:"BULAN_REALISASI"`
	TahunPMKBulanRealisasi sql.NullString `db:"TAHUN_PMK_BULAN_REALISASI"`
	TglJT                  string `db:"TGL_JT"`
	HistorisNonBayar       string         `db:"HISTORIS_NON_BAYAR"`
	Status                 string         `db:"STATUS"`
	OutstandingPrincipal   string `db:"OUTSTANDING_PRICIPAL"`
	TgAngsurTertunggak     string `db:"TG_ANGSUR_TERTUNGGAK"`
	TgRealisInputManual    string `db:"TG_REALIS_INPUT_MANUAL"`
	TypeAngsuran           string         `db:"TYPE_ANGSURAN"`
	CreatedBy              string         `db:"CREATED_BY"`
	CreatedTimestamp       string `db:"CREATED_TIMESTAMP"`
	ByrDenda               string         `db:"BYR_DENDA"`
	SldDenda               string         `db:"SLD_DENDA"`
	SourceOrder            string         `db:"SOURCE_ORDER"`
	PrdMonth               string         `db:"PRDMONTH"`
}

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "password"
	dbname   = "icos-dev"
)

const (
	dbname2 = "icos"
)

func main() {
	// username := "usersql"
	// password := "password"

	username := "anicos"
	password := "anicos"
	host := "192.168.51.173"
	// port := 1521
	serviceName := "prdfast"

	db, err := sqlx.Connect("godror", username+"/"+password+"@"+host+":1521/"+serviceName)
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
	}
	defer db.Close()

	// connection string
	// psqlconn1 := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	// db, err := sqlx.Open("postgres", psqlconn1)
	// if err != nil {
	// 	panic(err)
	// }

	// // close database
	// defer db.Close()

	// // Test the connection
	// err = db.Ping()
	// if err != nil {
	// 	log.Fatalf("Error pinging the database: %v", err)
	// }

	// log.Println("Successfully connected to the Oracle database!")

	// connection string
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname2)

	// open database
	dbpsql, err := sqlx.Open("postgres", psqlconn)
	if err != nil {
		panic(err)
	}

	// close database
	defer dbpsql.Close()

	// check db
	err = dbpsql.Ping()
	if err != nil {
		panic(err)
	}

	log.Println("Connected! Postgresql")

	sampleData := SampleData{}
	rows, err := db.Queryx("SELECT * FROM public.data_pkd")

	if err != nil {
		log.Fatalln(err)
	}

	i := 0

	createTable(dbpsql, "data_3")
	for rows.Next() {
		i++
		err = rows.StructScan(&sampleData)
		if err != nil {
			log.Fatalln(err)
		} 	

		fmt.Printf("row[ID]: %d row[Tanggal]: %d \n", i, sampleData.Umur)
		// // insertStmt := `INSERT INTO public.test ("id", "text") VALUES (1, "awda")`
		// _, error := dbpsql.Exec(`INSERT INTO public.test (id, name) VALUES (:1, :2);`, todo.Id, todo.Name)
		//_, error := dbpsql.Exec(`INSERT INTO public.data_pkd(
		// periode, _ := time.Parse(time.RFC3339, sampleData.DatePeriode)
		datePeriode, _ := convertToPostgreSQLDate(sampleData.DatePeriode)
		tanggalLahir, _ := convertToPostgreSQLDate(sampleData.TanggalLahir)
		tgRealis, _ := convertToPostgreSQLDate(sampleData.TgRealis)
		tgJTempoKontrak, _ := convertToPostgreSQLDate(sampleData.TgJTempoKontrak)
		tgAngsur, _ := convertToPostgreSQLDate(sampleData.TgAngsur)
		tgFixAng, _ := convertToPostgreSQLDate(sampleData.TgFixAng)
		if tgRealis == "" {
			tgRealis = "2023-11-27"
			} 
			if tgJTempoKontrak == "" {
				tgJTempoKontrak = "2023-11-27"
		} 
		if tgAngsur == "" {
			fmt.Println("TgAngsur is an empty string")
		} 
		if tgFixAng == "" {
			fmt.Println("TgFixAng is an empty string")
		}
		tgLunas, _ := convertToPostgreSQLDate(sampleData.TgLunas.String)
		if tgLunas == "" {
			tgLunas = "2023-11-27"
		}
		tglJT, _ := convertToPostgreSQLDate(sampleData.TglJT)
		historisNonBayar, _ := convertToPostgreSQLDate(sampleData.HistorisNonBayar)
		if historisNonBayar == "" {
			historisNonBayar = "2023-11-27"
		}
		tgAngsurTertunggak, _ := convertToPostgreSQLDate(sampleData.TgAngsurTertunggak)
		if len(tgAngsurTertunggak) < 1 {
			tgLunas = "2023-11-27"
		}
		tgRealisInputManual, _ := convertToPostgreSQLDate(sampleData.TgRealisInputManual)
		_, error := dbpsql.Exec(`INSERT INTO "dataPKD".data_3("DATEPERIODE", "OFFICE_CODE", "COY_ID", "REGIONAL_ID", "NAMA_CABANG", "CUST_ID", "CONTRACT_NO", "NAMA", "TANGGAL_LAHIR", "UMUR", "ALAMAT_KTP", "ALAMAT_TAGIH", "NO_FIX_PHONE", "NO_HANDPHONE", "ALAMAT_EMERGENCY", "NO_FIX_PHONE_EMERGENCY", "NO_HP_EMERGENCY", "ALAMAT_OFFICE", "NO_TELP_OFFICE", "NO_ZIP_CODE", "SUB_ZIP_CODE", "NAMA_KELURAHAN", "NAMA_KECAMATAN", "NAMA_KABUPATEN", "NAMA_PROVINSI", "JENIS", "TOP", "SUKUBUNGA", "TG_REALIS", "TG_JTEMPO_KONTRAK", "TG_ANGSUR", "TG_FIXANG", "PEKERJAAN", "KTP", "NAMA_SURVEYOR", "NAMA_KOLEKTOR", "NOMINAL_GROSS", "ADMIN", "ASURANSI", "NOMINAL_PENCAIRAN_NETT", "ANGSURAN_PER_BULAN", "ANG_POKOK", "ANG_BUNGA", "BYR_POKOK", "BYR_BUNGA", "SLD_POKOK", "SLD_BUNGA", "STATUS_KONTRAK", "TGLUNAS", "STATUS_ORDER", "JENIS_OBJ", "MERK_OBJ", "TYPE_OBJ", "NO_MESIN", "NO_RANGKA", "TAHUN", "WARNA", "NO_POLISI", "NO_BPKB", "NAMA_BPKB", "HARGA_PASAR_KENDARAAN", "PERSENTASE_PLAFON", "ANGS_TERAKHIR_BAYAR_KE", "ANGS_SHRNYA_BLN_INI", "JML_ANGS_TERTUNGGAK", "OD_HARI_INI", "KOL_HARI_INI", "CYCLE_HARI_INI", "OVER_JT_HARI_INI", "STATUS_NPL_HARI_INI", "STATUS_CNPL_HARI_INI", "AMBC_DENDA", "AMBC_POKOK", "AMBC_BUNGA", "TOTAL_KEWAJIBAN", "AC_DENDA", "AC_POKOK", "AC_BUNGA", "TOTAL_AC", "OD_AKHIR_BULAN", "KOL_AKHIR_BULAN", "CYCLE_AKHIR_BULAN", "OVER_JT_PER_AKHIR_BLN", "STS_NPL_CLSNG_AKHR_BLN", "STS_CNPL_CLSNG_AKHR_BLN", "OD_AWAL_BULAN", "KOL_AWAL_BULAN", "CYCLE_AWAL_BULAN", "OVER_JT_PER_AWAL_BULAN", "STATUS_NPL_AWAL_BULAN", "STATUS_CNPL_AWAL_BULAN", "TAHUN_PMK", "BULAN_REALISASI", "TAHUN_PMK_BULAN_REALISASI", "TGL_JT", "HISTORIS_NON_BAYAR", "STATUS", "OUTSTANDING_PRICIPAL", "TG_ANGSUR_TERTUNGGAK", "TG_REALIS_INPUT_MANUAL", "TYPE_ANGSURAN", "CREATED_BY", "CREATED_TIMESTAMP", "BYR_DENDA", "SLD_DENDA", "SOURCE_ORDER", "PRDMONTH")
										VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92, $93, $94, $95, $96, $97, $98, $99, $100, $101, $102, $103, $104, $105, $106, $107);`, datePeriode, sampleData.OfficeCode, sampleData.CoyID, sampleData.RegionalID, sampleData.NamaCabang,
			sampleData.CustID, sampleData.ContractNo, sampleData.Nama, tanggalLahir, sampleData.Umur,
			sampleData.AlamatKTP, sampleData.AlamatTagih, sampleData.NoFixPhone, sampleData.NoHandphone,
			sampleData.AlamatEmergency, sampleData.NoFixPhoneEmergency, sampleData.NoHPEmergency, sampleData.AlamatOffice,
			sampleData.NoTelpOffice, sampleData.NoZipCode, sampleData.SubZipCode, sampleData.NamaKelurahan,
			sampleData.NamaKecamatan, sampleData.NamaKabupaten, sampleData.NamaProvinsi, sampleData.Jenis, sampleData.Top,
			sampleData.SukuBunga, tgRealis, tgJTempoKontrak, tgAngsur, tgFixAng,
			sampleData.Pekerjaan, sampleData.KTP, sampleData.NamaSurveyor, sampleData.NamaKolektor, sampleData.NominalGross,
			sampleData.Admin, sampleData.Asuransi, sampleData.NominalPencairanNett, sampleData.AngsuranPerBulan,
			sampleData.AngPokok, sampleData.AngBunga, sampleData.ByrPokok, sampleData.ByrBunga, sampleData.SldPokok,
			sampleData.SldBunga, sampleData.StatusKontrak, tgLunas, sampleData.StatusOrder, sampleData.JenisObj,
			sampleData.MerkObj, sampleData.TypeObj, sampleData.NoMesin, sampleData.NoRangka, sampleData.Tahun, sampleData.Warna,
			sampleData.NoPolisi, sampleData.NoBPKB, sampleData.NamaBPKB, sampleData.HargaPasarKendaraan, sampleData.PersentasePlafon,
			sampleData.AngsTerakhirBayarKe, sampleData.AngsShrnyaBlnIni, sampleData.JmlAngsTertunggak, sampleData.ODHariIni,
			sampleData.KolHariIni, sampleData.CycleHariIni, sampleData.OverJtHariIni, sampleData.StatusNPLHariIni,
			sampleData.StatusCNPLHariIni, sampleData.AMBCDenda, sampleData.AMBCPokok, sampleData.AMBCBunga,
			sampleData.TotalKewajiban, sampleData.ACDenda, sampleData.ACPokok, sampleData.ACBunga, sampleData.TotalAC,
			sampleData.ODAkhirBulan, sampleData.KolAkhirBulan, sampleData.CycleAkhirBulan, sampleData.OverJtPerAkhirBln,
			sampleData.StsNPLClsngAkhrBln, sampleData.StsCNPLClsngAkhrBln, sampleData.ODAwalBulan, sampleData.KolAwalBulan,
			sampleData.CycleAwalBulan, sampleData.OverJtPerAwalBulan, sampleData.StatusNPLAwalBulan,
			sampleData.StatusCNPLAwalBulan, sampleData.TahunPMK, sampleData.BulanRealisasi, sampleData.TahunPMKBulanRealisasi,
			tglJT, historisNonBayar, sampleData.Status, sampleData.OutstandingPrincipal,
			tgAngsurTertunggak, tgRealisInputManual, sampleData.TypeAngsuran, sampleData.CreatedBy,
			sampleData.CreatedTimestamp, sampleData.ByrDenda, sampleData.SldDenda, sampleData.SourceOrder, sampleData.PrdMonth)
		if error != nil {
			log.Fatalf("error insert: %v", error)
		}
	}

}

func createTable(dbpsql *sqlx.DB, table string) {
	// Check if the table already exists
	queryCheck := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)")
	var exists bool
	err := dbpsql.Get(&exists, queryCheck, table)
	if err != nil {
		log.Fatal(err)
	}

	if !exists {
		// Create the table if it doesn't exist
		queryCreate := fmt.Sprintf(`CREATE TABLE "dataPKD"."%s" (
			"DATEPERIODE" date,
			"OFFICE_CODE" varchar(10) COLLATE "pg_catalog"."default",
			"COY_ID" varchar(5) COLLATE "pg_catalog"."default",
			"REGIONAL_ID" varchar(10) COLLATE "pg_catalog"."default",
			"NAMA_CABANG" varchar(255) COLLATE "pg_catalog"."default",
			"CUST_ID" varchar(20) COLLATE "pg_catalog"."default",
			"CONTRACT_NO" varchar(20) COLLATE "pg_catalog"."default",
			"NAMA" varchar(255) COLLATE "pg_catalog"."default",
			"TANGGAL_LAHIR" date,
			"UMUR" numeric(20,2),
			"ALAMAT_KTP" varchar(255) COLLATE "pg_catalog"."default",
			"ALAMAT_TAGIH" varchar(255) COLLATE "pg_catalog"."default",
			"NO_FIX_PHONE" varchar(20) COLLATE "pg_catalog"."default",
			"NO_HANDPHONE" varchar(20) COLLATE "pg_catalog"."default",
			"ALAMAT_EMERGENCY" varchar(255) COLLATE "pg_catalog"."default",
			"NO_FIX_PHONE_EMERGENCY" varchar(255) COLLATE "pg_catalog"."default",
			"NO_HP_EMERGENCY" varchar(20) COLLATE "pg_catalog"."default",
			"ALAMAT_OFFICE" varchar(255) COLLATE "pg_catalog"."default",
			"NO_TELP_OFFICE" varchar(20) COLLATE "pg_catalog"."default",
			"NO_ZIP_CODE" varchar(100) COLLATE "pg_catalog"."default",
			"SUB_ZIP_CODE" varchar(100) COLLATE "pg_catalog"."default",
			"NAMA_KELURAHAN" varchar(255) COLLATE "pg_catalog"."default",
			"NAMA_KECAMATAN" varchar(255) COLLATE "pg_catalog"."default",
			"NAMA_KABUPATEN" varchar(255) COLLATE "pg_catalog"."default",
			"NAMA_PROVINSI" varchar(255) COLLATE "pg_catalog"."default",
			"JENIS" int2,
			"TOP" int2,
			"SUKUBUNGA" numeric(30,2),
			"TG_REALIS" date,
			"TG_JTEMPO_KONTRAK" date,
			"TG_ANGSUR" date,
			"TG_FIXANG" date,
			"PEKERJAAN" varchar(50) COLLATE "pg_catalog"."default",
			"KTP" varchar(20) COLLATE "pg_catalog"."default",
			"NAMA_SURVEYOR" varchar(50) COLLATE "pg_catalog"."default",
			"NAMA_KOLEKTOR" varchar(50) COLLATE "pg_catalog"."default",
			"NOMINAL_GROSS" numeric(30,2),
			"ADMIN" numeric(30,2),
			"ASURANSI" numeric(30,2),
			"NOMINAL_PENCAIRAN_NETT" numeric(30,2),
			"ANGSURAN_PER_BULAN" numeric(30,2),
			"ANG_POKOK" numeric(30,2),
			"ANG_BUNGA" numeric(30,2),
			"BYR_POKOK" numeric(30,2),
			"BYR_BUNGA" numeric(30,2),
			"SLD_POKOK" numeric(30,2),
			"SLD_BUNGA" numeric(30,2),
			"STATUS_KONTRAK" varchar(50) COLLATE "pg_catalog"."default",
			"TGLUNAS" date,
			"STATUS_ORDER" varchar(50) COLLATE "pg_catalog"."default",
			"JENIS_OBJ" varchar(100) COLLATE "pg_catalog"."default",
			"MERK_OBJ" varchar(100) COLLATE "pg_catalog"."default",
			"TYPE_OBJ" varchar(100) COLLATE "pg_catalog"."default",
			"NO_MESIN" varchar(100) COLLATE "pg_catalog"."default",
			"NO_RANGKA" varchar(100) COLLATE "pg_catalog"."default",
			"TAHUN" varchar(4) COLLATE "pg_catalog"."default",
			"WARNA" varchar(100) COLLATE "pg_catalog"."default",
			"NO_POLISI" varchar(100) COLLATE "pg_catalog"."default",
			"NO_BPKB" varchar(50) COLLATE "pg_catalog"."default",
			"NAMA_BPKB" varchar(100) COLLATE "pg_catalog"."default",
			"HARGA_PASAR_KENDARAAN" numeric(30,2),
			"PERSENTASE_PLAFON" numeric(20,2),
			"ANGS_TERAKHIR_BAYAR_KE" int2,
			"ANGS_SHRNYA_BLN_INI" int2,
			"JML_ANGS_TERTUNGGAK" int2,
			"OD_HARI_INI" int2,
			"KOL_HARI_INI" int2,
			"CYCLE_HARI_INI" varchar(5) COLLATE "pg_catalog"."default",
			"OVER_JT_HARI_INI" varchar(100) COLLATE "pg_catalog"."default",
			"STATUS_NPL_HARI_INI" varchar(100) COLLATE "pg_catalog"."default",
			"STATUS_CNPL_HARI_INI" varchar(100) COLLATE "pg_catalog"."default",
			"AMBC_DENDA" numeric(30,2),
			"AMBC_POKOK" numeric(30,2),
			"AMBC_BUNGA" numeric(30,2),
			"TOTAL_KEWAJIBAN" numeric(30,2),
			"AC_DENDA" numeric(30,2),
			"AC_POKOK" numeric(30,2),
			"AC_BUNGA" numeric(30,2),
			"TOTAL_AC" numeric(30,2),
			"OD_AKHIR_BULAN" int2,
			"KOL_AKHIR_BULAN" int2,
			"CYCLE_AKHIR_BULAN" int2,
			"OVER_JT_PER_AKHIR_BLN" varchar(100) COLLATE "pg_catalog"."default",
			"STS_NPL_CLSNG_AKHR_BLN" varchar(100) COLLATE "pg_catalog"."default",
			"STS_CNPL_CLSNG_AKHR_BLN" varchar(100) COLLATE "pg_catalog"."default",
			"OD_AWAL_BULAN" int2,
			"KOL_AWAL_BULAN" int2,
			"CYCLE_AWAL_BULAN" varchar(5) COLLATE "pg_catalog"."default",
			"OVER_JT_PER_AWAL_BULAN" varchar(100) COLLATE "pg_catalog"."default",
			"STATUS_NPL_AWAL_BULAN" varchar(100) COLLATE "pg_catalog"."default",
			"STATUS_CNPL_AWAL_BULAN" varchar(100) COLLATE "pg_catalog"."default",
			"TAHUN_PMK" varchar(50) COLLATE "pg_catalog"."default",
			"BULAN_REALISASI" varchar(50) COLLATE "pg_catalog"."default",
			"TAHUN_PMK_BULAN_REALISASI" varchar(255) COLLATE "pg_catalog"."default",
			"TGL_JT" date,
			"HISTORIS_NON_BAYAR" date,
			"STATUS" varchar(50) COLLATE "pg_catalog"."default",
			"OUTSTANDING_PRICIPAL" numeric(30,2),
			"TG_ANGSUR_TERTUNGGAK" date,
			"TG_REALIS_INPUT_MANUAL" date,
			"TYPE_ANGSURAN" varchar(100) COLLATE "pg_catalog"."default",
			"CREATED_BY" varchar(100) COLLATE "pg_catalog"."default",
			"CREATED_TIMESTAMP" timestamp(6),
			"BYR_DENDA" numeric(30,2),
			"SLD_DENDA" numeric(30,2),
			"SOURCE_ORDER" varchar(100) COLLATE "pg_catalog"."default",
			"PRDMONTH" int2
		  )
			  ;
			`, table)
		_, err := dbpsql.Exec(queryCreate)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Table successfully created.")
	} else {
		fmt.Println("Table already exists.")
	}
}

func convertToPostgreSQLDate(inputTimestamp string) (string, error) {
	// Parse the input timestamp string
	parsedTime, err := time.Parse(time.RFC3339, inputTimestamp)
	if err != nil {
		return "", fmt.Errorf("error parsing timestamp: %v", err)
	}

	// Format the date according to PostgreSQL date format
	postgreSQLDate := parsedTime.Format("2006-01-02")

	return postgreSQLDate, nil
}
