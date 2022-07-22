<?PHP
error_reporting(E_ALL & ~E_NOTICE);
ini_set('memory_limit','-1');
define("LOGPATH", "./logs/");
define("OUTPUTPATH", "./output/");
define("HEAD", "Content-Type: application/json");

#ES
define("ES_HOST", "http://localhost:9200");
define("ES_AUTH", "elastic:password");
define("ES_INDEX", "logs-localdata-accommodation");

# KAKAO API 
define("KAKAO_REST_API_KEY1", "********************************");
define("KAKAO_REST_API_KEY2", "********************************");
define("KAKAO_ADDRESS_API_URL", "https://dapi.kakao.com/v2/local/search/address.json");
define("KAKAO_COORD2REGIONCODE_API_URL", "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json");
define("KAKAO_TRANSCOORD_API_URL", "https://dapi.kakao.com/v2/local/geo/transcoord.json");
define("KAKAO_KEYWORD_API_URL", "https://dapi.kakao.com/v2/local/search/keyword.json");
define("KAKAO_CATEGORY_API_URL", "https://dapi.kakao.com/v2/local/search/category.json");
define("KAKAO_TRANSCOORD_INPUT", "TM"); #EPGS:2097
define("KAKAO_TRANSCOORD_OUTPUT", "WGS84");#EPGS:4326
# @TODO NAVER API 
define("NVAVER_ADDRESS_API_URL", "https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode");
define("NVAVER_API_CLIENT_ID", "X-NCP-APIGW-API-KEY-ID: {}");
define("NVAVER_API_CLIENT_KEY", "X-NCP-APIGW-API-KEY: {}");

/**
 * @var string
 */
global $KAKAO_KEY;//다중키 사용을 위한 전역변수

/**
 * getHelp
 * CLI ANSI HELP
 * @return string
 */
function getHelp() {  
  return <<<EOT
  \033[H\033[2J
  \033[32mGIS CSV 읽어서 카카오API로 좌표계 보정 및 데이터 병합후 ES 벌크 인서트\033[0m \n
  \033[93m사용방법:\033[0m php \033[34 mmul.php\033[0m [\033[92m csv파일\033[0m] \n
  EOT;
}

if ($argc < 2 || in_array($argv[1], array('--help', '-help', '-h', '-?'))) {
  echo getHelp();exit;
} else {   
  $csv=csv_read($argv[1]);
  csv_parser($csv);
  exit;
}

/**
 * read_csv
 *
 * @param  mixed $csv
 * @return void
 */
function csv_read($csvfile)
{  
  return (file_exists($csvfile)) ? file_get_contents($csvfile) : false;
}

/**
 * csv_parser
 *
 * @param  mixed $csv
 * @return void
 */
function csv_parser($csv)
{  
  $title = array("no","service_name","service_id","org_code","mng_no","license_reg_date","license_cancel_date","state_code","state_name","state_detail_code","state_detail_name","shutdown_date","closed_start_date","closed_end_date","reopen_date","site_tel","site_area","site_post","site_address","road_address","road_post","business_name","last_update","update_type","update_date","business_type","x","y","sanitary_business_name","building_ground","building_underground","use_start_ground","use_end_ground","use_start_underground","use_end_underground","krean_rooms","western_rooms","bath_rooms","sweating_rooms","seat","conditional_perm_reason","conditional_perm_start_date","conditional_perm_end_date","building_owned","washing_machine","female_employee","male_employee","drying_machine","bed","multi_use_business","coordinates");
  if (!empty($csv)) {
    foreach (explode("\n",$csv) as $row => $rowdata) {      
      // 컬럼안의 콤마를 ^로 변환시켜 우회
      foreach (explode(",",$rowdata) as $column => $value) {      
        if ($row>0) {
          $KAKAO_KEY = ($row % 2 == 1) ? KAKAO_REST_API_KEY1:KAKAO_REST_API_KEY2; 
          $$title[$column] = (!empty($title[$column]))? column_valid($title[$column],$value):null;
          $result[$row][$title[$column]] = $$title[$column];
          if ($title[$column]=="site_address") {
            if (!empty($$title[$column])) {
              $address_kakao = json_decode(kakao_local_search(array("query"=>$$title[$column]),$KAKAO_KEY),true);
            }
          }          
          if ($title[$column]=="road_address") {
            if (!empty($$title[$column])) {
              $address_kakao = json_decode(kakao_local_search(array("query"=>$$title[$column]),$KAKAO_KEY),true);
            }
          } 
        }
      }//endof foreach row

      if ($row>0) { 
        if ($address_kakao["meta"]["total_count"]) {
          $json["kakao"] = $address_kakao["documents"];
          $jsondata =json_encode(array_merge($result[$row],$json),JSON_UNESCAPED_UNICODE).PHP_EOL;        
        }else{ //$address_kakao 없다면
          if ($result[$row]["x"]&&$result[$row]["y"]) {
            $query =array("x"=>$result[$row]["x"],"y"=>$result[$row]["y"],"input_coord"=>KAKAO_TRANSCOORD_INPUT,"output_coord"=>KAKAO_TRANSCOORD_OUTPUT);
            $geo_kakao = json_decode(kakao_transcoord_search($query,$KAKAO_KEY),true);
            if ($geo_kakao["meta"]["total_count"]) {            
              $json["coordinates"] = array($geo_kakao["documents"][0]["x"],$geo_kakao["documents"][0]["y"]);
              $jsondata =json_encode(array_merge($result[$row],$json),JSON_UNESCAPED_UNICODE).PHP_EOL;    
            }          
          }
          $jsondata =json_encode($result[$row],JSON_UNESCAPED_UNICODE).PHP_EOL;        
        }

        $bulkdata= set_index(ES_INDEX).$jsondata;//저장데이터 출력
        //echo "[$row] KAKAO_KEY: $KAKAO_KEY".PHP_EOL;//홀짝 카카오API 디버그 // 일일 10만 한계때문에 2개 키를 사용함.
        if (!empty($jsondata)) print_r($jsondata);  //ES 저장데이터 확인용
        log_write($bulkdata,$row); //입력데이터 로그 저장
        $ret= curl_post(ES_HOST ."/_bulk",$bulkdata).PHP_EOL; // 1건식 ES 입력
        log_write($ret,$row);//ES 결과 로그 저장
        file_write($bulkdataa,ES_INDEX.".json"); // 재사용을 위해 BULK DATA 저장
        echo '[ [38;5;190m' . date("Y-m-d H:i:s") . '[0m ][[033;92m'. $row.'[0m] '.$ret.PHP_EOL;//결과 모니터링
      } 
      //if ($row>9) exit;
    } //endof foreach csv  
  }
}

/**
 * set_index
 *
 * @param  string $index 인덱스
 * @param  string $id 아이디
 * @return json
 */
function set_index($index,$id=null) 
{
  return ($id)?
   json_encode(array("index"=> array("_index"=>"$index","_id"=>"$id"))).PHP_EOL:
   json_encode(array("index"=> array("_index"=>"$index",))).PHP_EOL;
}

/**
 * column_valid
 *
 * @param  mixed $column
 * @param  mixed $str
 * @return void
 */
function column_valid($column,$str) {
  switch($column)
  {
    //Ymd
    case 'license_reg_date';
    case 'license_cancel_date';
    case 'shutdown_date';
    case 'closed_start_date';
    case 'closed_end_date';
    case 'reopen_date';
    case 'conditional_perm_start_date';
    case 'conditional_perm_end_date';
      return char_valid($str,"date","Ymd");
      break;
    case 'last_update';
      return char_valid($str,"date","Ymdhhiiss");
      break;    
    case 'update_date';
      return char_valid($str,"date","Y-m-d hh:ii:ss.S");
      break;         
    //int
    case 'no';
    case 'site_post';
    case 'road_post';
    case 'building_ground';
    case 'building_underground';
    case 'use_start_ground';
    case 'use_end_ground';
    case 'use_start_underground';
    case 'use_end_underground';
    case 'krean_rooms';
    case 'western_rooms';
    case 'bath_rooms';
    case 'seat';
    case 'washing_machine';
    case 'female_employee';
    case 'male_employee';
    case 'drying_machine';
    case 'bed';
      return char_valid($str,"int","0");
      break;
    //string
    case 'service_name';
    case 'service_id';
    case 'org_code';
    case 'mng_no';
    case 'state_code';
    case 'state_name';
    case 'state_detail_code';
    case 'state_detail_name';
    case 'site_tel';
    case 'site_address';
    case 'road_address';
    case 'business_name';
    case 'update_type';    
    case 'business_type';    
    case 'sanitary_business_name';     
    case 'conditional_perm_reason';    
    case 'multi_use_business';  
    return char_valid($str,"string","");      
      break;
    case 'sweating_rooms';         
    case 'building_owned';          
    return char_valid($str,"string","N");      
      break;
    //float
    case 'site_area';
    case 'x';
    case 'y';
    return char_valid($str,"float","");    
      break;
  }
}

/**
 * char_valied
 *
 * @param  mixed $str
 * @param  mixed $type
 * @param  mixed $format
 * @return void
 */
function char_valid($str,$type="string",$format="") {
  switch($type)
  {
    case 'int';
      return (int)preg_replace("/[^0-9]*/s", "", $str);
      break;      
    case 'date';
      switch($format) {
        case 'Ymd';
        if (strlen(trim($str))==6) {
          return trim($str)."01";
        } else if (strlen(trim($str))==8) {
          return trim($str);
        } else {
          return null;
        }        
        break;  
        case 'Ymdhhiiss';
        return (strlen(trim($str))==14) ? trim($str) :null;
        break;    
        case 'Y-m-d hh:ii:ss.S';
        return (strlen(trim($str))==21) ? trim($str) :null;
        break;                          
      }
      return trim($str);
      break;      
    case 'float';//site_area 천단위 콤마를 ^로 변환시킴
      return (empty($str))?null:(float)str_replace('^','',trim($str));
      break;
    default;
      return (empty($str))?$format:str_replace(array('"','^'),'',trim($str));  
      break;
  }
}

/**
 * kakao_local_search
 *
 * @param  mixed $get
 * @param  string $key
 * @param  mixed $options
 * @return void
 */
function kakao_local_search(array $get = NULL,string $key="", array $options = array())
{   
    if (empty($key)||empty($get)) return false;
    $defaults = array(
        CURLOPT_URL             => KAKAO_ADDRESS_API_URL.(strpos(KAKAO_ADDRESS_API_URL, '?') === FALSE ? '?' : ''). ((isset($get))?http_build_query($get):'')
        ,CURLOPT_HEADER         => 0        
        ,CURLOPT_HTTPHEADER     => array(HEAD,"Authorization: KakaoAK ".$key)
        ,CURLOPT_RETURNTRANSFER => 1
        ,CURLOPT_TIMEOUT        => 4
    );
   
    $ch = curl_init();
    curl_setopt_array($ch, ($options + $defaults));
    if( ! $result = curl_exec($ch)) trigger_error(curl_error($ch));
    curl_close($ch);
    return $result;
}

/**
 * kakao_transcoord_search
 *
 * @param  mixed $get
 * @param  string $key
 * @param  mixed $options
 * @return void
 */
function kakao_transcoord_search(array $get = NULL,string $key="", array $options = array())
{ 
  if (empty($key)||empty($get)) return false;  
  $defaults = array(
    CURLOPT_URL             => KAKAO_TRANSCOORD_API_URL.(strpos(KAKAO_TRANSCOORD_API_URL, '?') === FALSE ? '?' : ''). ((isset($get))?http_build_query($get):'')
    ,CURLOPT_HEADER         => 0        
    ,CURLOPT_HTTPHEADER     => array(HEAD,"Authorization: KakaoAK ".$key)
    ,CURLOPT_RETURNTRANSFER => 1
    ,CURLOPT_TIMEOUT        => 4
  );

  $ch = curl_init();
  curl_setopt_array($ch, ($options + $defaults));
  if( ! $result = curl_exec($ch)) trigger_error(curl_error($ch));
  curl_close($ch);
  return $result;   
}

/**
* Send a POST requst using cURL
* @param string $url to request
* @param array $post values to send
* @param array $options for cURL
* @return string
*/
function curl_post($url, $post = NULL, array $options = array())
{
    $defaults = array(
        CURLOPT_POST            => 1
        ,CURLOPT_HEADER         => 0
        ,CURLOPT_HTTPHEADER     => array(HEAD)
        ,CURLOPT_URL            => $url
        ,CURLOPT_USERPWD        => ES_AUTH
        ,CURLOPT_FRESH_CONNECT  => 1
        ,CURLOPT_RETURNTRANSFER => 1
        ,CURLOPT_FORBID_REUSE   => 1
        ,CURLOPT_TIMEOUT        => 4
        ,CURLOPT_POSTFIELDS     => $post
    );

    $ch = curl_init();
    curl_setopt_array($ch, ($options + $defaults));
    if( ! $result = curl_exec($ch)) trigger_error(curl_error($ch));
    curl_close($ch);
    return $result;
}

/**
* Send a GET requst using cURL
* @param string $url to request
* @param array $get values to send
* @param array $options for cURL
* @return string
*/
function curl_get($url, array $get = NULL, array $options = array())
{   
    $defaults = array(
        CURLOPT_URL             => $url. (strpos($url, '?') === FALSE ? '?' : ''). ((isset($get))?http_build_query($get):'')
        ,CURLOPT_HEADER         => 1
        ,CURLOPT_HTTPHEADER     => array(HEAD)
        ,CURLOPT_USERPWD        => ES_AUTH
        ,CURLOPT_RETURNTRANSFER => 1
        ,CURLOPT_TIMEOUT        => 4
    );
   
    $ch = curl_init();
    curl_setopt_array($ch, ($options + $defaults));
    if( ! $result = curl_exec($ch)) trigger_error(curl_error($ch));
    curl_close($ch);
    return $result;
}

/**
 * log_write
 *
 * @param  mixed $str
 * @param  mixed $level
 * @return void
 */
function log_write($str,$level="info")
{
  if (!is_dir(LOGPATH)) mkdir(LOGPATH, 0644, true);
  $filename = date("Ymd").'.log';  
  $log = '[ [38;5;190m' . date("Y-m-d H:i:s") . '[0m ][[033;92m'. $level.'[0m] ' . $str . PHP_EOL;  
  $log_file = fopen(LOGPATH.$filename, 'a+');
  fwrite($log_file, $log);
  fclose($log_file);
}

/**
 * save_file
 *
 * @param  mixed $data
 * @param  mixed $filename
 * @return void
 */
function file_write($data,$filename)
{  
  if (!is_dir(OUTPUTPATH)) mkdir(OUTPUTPATH, 0644, true);  
  $handle = fopen(OUTPUTPATH.$filename, 'a+');
  fwrite($handle, $data);
  fclose($handle);
}
