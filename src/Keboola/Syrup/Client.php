<?php
namespace Keboola\Syrup;

use Guzzle\Common\Collection;
use Guzzle\Plugin\Backoff\BackoffPlugin;
use Guzzle\Service\Description\ServiceDescription;

/**
 * Class Client
 * @package Keboola\Orchestrator
 */
class Client extends \Guzzle\Service\Client
{
    const DEFAULT_API_URL = 'https://syrup.keboola.com';

    protected $maxRetries = 20;

    protected $jobFinishedStates = array("cancelled", "canceled", "success", "error");

    protected $maxDelay = 300;

    /**
     * @var string parent component, eg https://syrup.keboola.com/docker
     */
    protected $super = '';

    /**
     * @param array $config
     * @return Client
     */
    public static function factory($config = array())
    {
        $default = array(
            'url' => static::DEFAULT_API_URL,
        );
        $required = array('token');
        $config = Collection::fromConfig($config, $default, $required);
        $config['curl.options'] = array(
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_0,
        );
        $config['request.options'] = array(
            'headers' => array(
                'X-StorageApi-Token' => $config->get('token')
            )
        );

        $client = new static($config->get('url'), $config);

        // Attach a service description to the client
        $description = ServiceDescription::factory(__DIR__ . '/service.json');
        $client->setDescription($description);
        $client->setBaseUrl($config->get('url'));

        // Setup exponential backoff
        $backoffPlugin = BackoffPlugin::getExponentialBackoff();
        $client->addSubscriber($backoffPlugin);

        if ($config->get("super")) {
            $client->super = $config->get("super");
        }

        return $client;
    }

    /**
     * Create a new async job
     *
     * Available options are
     *  - config - configuration id
     *  - configData - configuration data
     *
     * @param $component
     * @param array $options
     * @return mixed
     */
    public function createJob($component, $options = array())
    {
        $params = $options;
        $params["component"] = $component;
        $params["super"] = $this->super;
        $command = $this->getCommand('CreateJob', $params);
        return $command->execute();
    }

    /**
     *
     * Get async job status
     *
     * @param $job
     * @return mixed
     */
    public function getJob($job)
    {
        $params = array(
            "job" => $job
        );
        return $this->getCommand('GetJob', $params)->execute();
    }

    /**
     * @param $component
     * @param array $options
     * @return mixed
     * @throws ClientException
     */
    public function runJob($component, $options = array())
    {
        $response = $this->createJob($component, $options);
        if (!isset($response["id"])) {
            throw new ClientException("Invalid response.");
        }
        $finished = false;
        $attempt = 0;
        $job = array();
        while (!$finished) {
            $job = $this->getJob($response["id"]);
            if (in_array($job["status"], $this->jobFinishedStates)) {
                $finished = true;
            }
            $attempt++;
            if ($attempt > $this->maxRetries && !$finished) {
                throw new ClientException("Too many retries.");
            }
            sleep(min(2 ^ $attempt, $this->maxDelay));
        }
        return $job;
    }
}
