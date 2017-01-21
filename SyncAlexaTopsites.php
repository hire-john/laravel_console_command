<?php

namespace yashi\Console\Commands;

use Illuminate\Console\Command;
use yashi\Http\Controllers\AlexaDomainsController;
use yashi\Http\Controllers\AlexaWebInfoServices;

class SyncAlexaTopsites extends Command {

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'sync:alexatopsites';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Grabs the top 500 ranked global websites from Alexa and stores into datbase';
    private $requestIterationNumbers;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct() {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * Prep objects and methods
     * Determine if a fresh write or fragmented write should occur
     * based on hash counts. 
     * 
     * @return nothing
     */
    public function handle() {
        $this->requestIterationNumbers['startNum'] = 1;
        $this->requestIterationNumbers['sites'] = 253;
        $this->calculateIterations();
        $keyId = ATS_KEYID; /* see env */
        $keyValue = ATS_KEY; /* see env */
        $awis = new AlexaWebInfoServices($keyId, $keyValue);
        $ats = new AlexaDomainsController();
        $hashes = $ats->selectUrlHashes();
        $hashCounts = count($hashes) - $this->requestIterationNumbers['iterations'];
        if (count($hashes) == 0 || $hashCounts !== 0) {
            $ats->cleanDomainAndHashTables();
            $this->executeInitialTransfer($awis, $ats);
        } else {
            $this->executeRollingTransfer($awis, $ats, $hashes);
        }
    }

    /*
     * if hash counts differ or don't exist wipe 
     * all command automated rows and perform fresh writes
     */

    private function executeInitialTransfer($awis, $ats) {
        for ($i = 0; $i < $this->requestIterationNumbers['iterations']; $i++) {
            $this->updateRequestNumbers();
            $result = $awis->getTopSites($this->requestIterationNumbers['quantity'], $this->requestIterationNumbers['startNum']);
            $ats->automatedAlexaInsertion($result);
            $ats->storeUrlHash($awis->alexaUrlHash, (int) 1);
            $this->requestIterationNumbers['startNum'] = $this->requestIterationNumbers['startNum'] + 100;
            $this->requestIterationNumbers['sites'] = $this->requestIterationNumbers['sites'] - $this->requestIterationNumbers['quantity'];
        }
    }

    /*
     * compare hash of pulled data with database hash skip chuck if match
     * to reduce database writes
     */

    private function executeRollingTransfer($awis, $ats, $hashes) {
        for ($i = 0; $i < $this->requestIterationNumbers['iterations']; $i++) {
            $this->updateRequestNumbers();
            $result = $awis->getTopSites($this->requestIterationNumbers['quantity'], $this->requestIterationNumbers['startNum']);
            $test = $this->compareHashes($hashes[$i]->hash, $awis->alexaUrlHash);
            if ($test !== 0) {
                $ats->rollingAlexaInsertion($result, $this->requestIterationNumbers);
                $ats->updateUrlHash($awis->alexaUrlHash, $hashes[$i]->order);
            }
        }
    }

    /*
     * method to compare existing hashes vs new hash created from api data
     */

    private function compareHashes($dbHash, $currentHash) {
        return strcmp($dbHash, $currentHash);
    }

    /*
     *  calculate and modify variables required to iterate
     * method to pull x amount of sites from api
     */

    private function updateRequestNumbers() {
        $sites = $this->requestIterationNumbers['sites'];
        if ($sites > 100) {
            $this->requestIterationNumbers['quantity'] = 100;
        } else {
            $this->requestIterationNumbers['quantity'] = $this->requestIterationNumbers['sites'];
        }
    }

    /*
     * calculate iterations required to fullfill request to pull
     * x amount sites in relation with 100 site per request limit
     * from Alexa Top Sites
     */

    private function calculateIterations() {
        $sites = $this->requestIterationNumbers['sites'];
        if ($sites > 100) {
            $remainder = $sites % 100;
            $dividend = ($sites - $remainder) / 100;
            if ($remainder > 0) {
                $dividend = $dividend + 1;
            }
            $this->requestIterationNumbers['iterations'] = $dividend;
        } else {
            $this->requestIterationNumbers['iterations'] = 1;
        }
    }

    private function automatedAlexaInsertion($results) {
        $urlCount = count($results['url']);
        $rankCount = count($results['rank']);
        if ($urlCount == $rankCount) {
            for ($i = 0; $i < $urlCount; $i++) {
                $url = $results['url'][$i];
                $rank = $results['rank'][$i];
                $this->insertDomain($url, $rank, true);
            }
        }
    }

    private function rollingAlexaInsertion($results, $rankConditions) {
        $urlCount = count($results['url']);
        $rankCount = count($results['rank']);
        if ($urlCount == $rankCount) {
            $max = $rankConditions['startNum'] + $rankConditions['quantity'];
            $min = $rankConditions['startNum'];
            DB::table('domains')->where('rank', '<', $max)->where('rank', '>', $min)->delete();
            for ($i = 0; $i < $urlCount; $i++) {
                $url = $results['url'][$i];
                $rank = $results['rank'][$i];
                $this->insertDomain($url, $rank, true);
            }
        }
    }

    private function selectUrlHashes() {
        return DB::table('alexaUrlHashes')->get();
    }

    private function cleanDomainAndHashTables() {
        DB::table('domains')->where('aggregated', '=', true)->delete();
        DB::table('alexaUrlHashes')->delete();
    }

    private function storeUrlHash($hash, $order) {
        DB::table("alexaUrlHashes")->insert(
                [
                    'hash' => $hash,
                    'order' => $order,
                ]
        );
    }

    private function insertDomain($url, $rank, $aggregated = false) {
        DB::table("domains")->insert(
                [
                    'domain' => $url,
                    'rank' => $rank,
                    'aggregated' => $aggregated,
                ]
        );
    }

}
