<?php

class CallsGateway
{
    private PDO $conn;

    public function __construct(Database $database){
        $this->conn = $database->getConnection();
    }

    public function checkRequest($phone): bool
    {
        $sql = <<<sql
            select count(*) from data.calls where date = current_date and phone = :phone and actual_call = true;
        sql;

        $stmt = $this->conn->prepare($sql);
        $stmt->bindValue(":phone", $phone, PDO::PARAM_STR);
        $stmt->execute();
        $res = $stmt->fetch(PDO::FETCH_NUM)[0];

        if($res == 1){
            return true;
        }else{
            return false;
        }
    }

    public function getCallResponseDetails($phone): array | bool
    {
        $sql = <<<sql
            select site, language, call_type, curr_call, phone_type from data.calls where date = current_date and phone = :phone and actual_call = true;
        sql;

        $stmt = $this->conn->prepare($sql);
        $stmt->bindValue(":phone", $phone, PDO::PARAM_STR);
        $stmt->execute();
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        return $result;
    }

    public function putCallDetails($phone, $status, $duration): bool
    {
        $answer = true;
        if($status === "NotAnswered"){
            $answer = false;
        }
        
        $callDetails = $this->getCallResponseDetails($phone); //get call information from database
        if(!$callDetails or count($callDetails) < 1){
            return false;
        }

        $morning_sql = <<<sql
            with rows as (
                update data.calls
                set morning_answer = :answer_binary, morning_duration = :duration, morning_endstatus = :status, morning_endtime = now()
                where phone = :phone and date = current_date and actual_call = true
                returning 1
            )
            select count(*) from rows;
        sql;

        $afternoon_sql = <<<sql
            with rows as (
                update data.calls
                set afternoon_answer = :answer_binary, afternoon_duration = :duration, afternoon_endstatus = :status, afternoon_endtime = now()
                where phone = :phone and date = current_date and actual_call = true
                returning 1
            )
            select count(*) from rows;
        sql;

        if($callDetails["curr_call"] == "morning"){
            $stmt = $this->conn->prepare($morning_sql);
        }else{
            $stmt = $this->conn->prepare($afternoon_sql);
        }
        $stmt->bindValue(":phone", $phone, PDO::PARAM_STR);
        $stmt->bindValue(":status", $status, PDO::PARAM_STR);
        $stmt->bindValue(":duration", $duration, PDO::PARAM_STR);
        $stmt->bindValue(":answer_binary", $answer, PDO::PARAM_BOOL);
        $stmt->execute();
        $res = $stmt->fetch(PDO::FETCH_NUM)[0];

        if($res == 1){
            return true;
        }else{
            return false;
        }
    }

    public function putAudioMetadata($curr_call, $audio_file_exists, $audio_file_name, $phone) :void
    {
        //Log audio file name and whether it existed
        $sql = "update data.calls set {$curr_call}_audio_file_existed = :exists, {$curr_call}_audio_file_name = :filename where phone = :phone and date = current_date and actual_call = true;";
        $stmt = $this->conn->prepare($sql);
        $stmt->bindValue(":exists", $audio_file_exists, PDO::PARAM_BOOL);
        $stmt->bindValue(":filename", $audio_file_name, PDO::PARAM_STR);
        $stmt->bindValue(":phone", $phone, PDO::PARAM_STR);
        $stmt->execute();
    }
}