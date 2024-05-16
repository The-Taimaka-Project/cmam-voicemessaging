<?php

class CallsController
{
    public function __construct(private CallsGateway $gateway){}

    public function processRequest(string $method): void
    {
        //Terminate all requests that are not post requests
        if($method !== "POST"){
            http_response_code(405);
            exit;
        }

        if($_POST["isActive"] == 1){ //check whether this is a final request for ended call

            //Debugging
            // file_put_contents(__DIR__ . "/../text.txt", file_get_contents('php://input'));

            header('Content-type: text/plan');
            //Reject all incoming calls
            if($_POST["direction"] === "inbound"){
                $this->rejectCall($_POST["callerNumber"]);
                return;
            }

            //Reject call if not uniquely valid date + phone combination - SHOULD ADD LOGGING AT SOME POINT
            // file_put_contents(__DIR__ . "/../text.txt", "before check\n", FILE_APPEND);
            $valid = $this->gateway->checkRequest($_POST["callerNumber"]);
            // file_put_contents(__DIR__ . "/../text.txt", "got to check\n", FILE_APPEND);
            if(!$valid){
                $this->rejectCall($_POST["callerNumber"]);
                return;
            }

            $fallback = "other_hausa_kuri_wk1"; //highest order fallback for failed audio file name construction

            //Check whether client request id is valid -> play fallback audio if fails
            $details = $this->gateway->getCallResponseDetails($_POST["callerNumber"]);
            if(!$details or count($details) < 1){
                $this->playAudio($fallback);
                return;
            }

            // file_put_contents(__DIR__ . "/../text.txt", "got passed response details check\n", FILE_APPEND);
            //Set audio file name
            //Phone owner
            if($details["phone_type"] === "own" || $details["phone_type"] === "other"){
                $ass_phone_part = $details["phone_type"];
            }else{
                $ass_phone_part = "other";
            }
            //Language
            if($details["language"] === "hausa" || $details["language"] === "fulfulde"){
                $ass_language_part = $details["language"];
            }else{
                $ass_language_part = "hausa";
            }
            //Site
            $valid_sites = array("kuri", "kurjale", "jalingo", "sangaru");
            if(in_array($details["site"], $valid_sites)){
                $ass_site_part = $details["site"];
            }else{
                if($ass_language_part === "fulfulde"){
                    $ass_site_part = "jalingo";
                }else{
                    $ass_site_part = "kuri";
                }
            }
            //Call type
            $valid_types = array("drop", "wk1", "wk2", "wk3", "wk4");
            if(in_array($details["call_type"], $valid_types)){
                $ass_type_part = $details["call_type"];
            }else{
                $ass_type_part = "wk4";
            }
            //Construct final name
            $file_name = $ass_phone_part . "_" . $ass_language_part . "_" . $ass_site_part . "_" . $ass_type_part;
            // file_put_contents(__DIR__ . "/../text.txt", $file_name, FILE_APPEND);

            //Check if constructed file exists
            $b_file_exists = file_exists("/var/www/api/audio/".$file_name.".wav");
            //Write this information to database
            $this->gateway->putAudioMetadata($details["curr_call"], $b_file_exists, $file_name, $_POST["callerNumber"]);

            if($b_file_exists){
                $this->playAudio($file_name);
            }else{
                $this->playAudio($fallback);
            }

            return;
        }else{
            $this->gateway->putCallDetails($_POST["callerNumber"], $_POST["callSessionState"], $_POST["durationInSeconds"]);
            //TO-DO - add error handlers
            return;
        }
    }

    private function playAudio($filename): void
    {
        $base_url = "https://api.taimaka-internal.org:40/audio/";
        $response  = '<?xml version="1.0" encoding="UTF-8"?>';
        $response .= '<Response>';
        $response .= '<Play url="' . $base_url.$filename.".wav" . '"/>';
        $response .= '</Response>';
        echo $response;
    }

    private function rejectCall($arg): void //SHOULD ADD LOGGING AT SOME POINT
    {
        $response  = '<?xml version="1.0" encoding="UTF-8"?>';
        $response .= '<Response>';
        // $response .= '<Say voice="en-US-Standard-C" playBeep="false">'.$arg.'</Say>';
        $response .= '<Say voice="en-US-Standard-C" playBeep="false">Invalid caller.</Say>';
        $response .= '<Reject/>';
        $response .= '</Response>';
        echo $response;
    }
}