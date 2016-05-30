<?php

namespace Keboola\Syrup;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Uri;
use GuzzleHttp\Client as GuzzleClient;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

/**
 * Class Client
 * @package Keboola\Orchestrator
 */
class Client
{
    const DEFAULT_API_URL = 'https://syrup.keboola.com';
    const DEFAULT_USER_AGENT = 'Keboola Syrup PHP Client';
    const DEFAULT_BACKOFF_RETRIES = 11;

    protected $jobFinishedStates = ["cancelled", "canceled", "success", "error", "terminated"];

    /**
     * @var int Maximum delay between queries for job state
     */
    protected $maxDelay = 10;

    /**
     * @var string Name of parent component
     */
    protected $super = '';

    /*
     * @var string Actual base request URL.
     */
    private $url;

    /**
     * GuzzleClient
     */
    protected $guzzle;

    private static function createDefaultDecider($maxRetries = 3)
    {
        return function (
            $retries,
            RequestInterface $request,
            ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() > 499) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }

    /**
     * Create a client instance
     *
     * @param array $config Client configuration settings:
     *     - token: (optional) Storage API token.
     *     - runId: (optional) Storage API runId.
     *     - url: (optional) Syrup API URL to override the default (DEFAULT_API_URL).
     *     - super: (optional) Name of parent component if any.
     *     - userAgent: (optional) Custom user agent (appended to the default).
     *     - backoffMaxTries: (optional) Number of retries in case of backend error.
     *     - logger: (optional) instance of Psr\Log\LoggerInterface.
     *     - handler: (optional) instance of GuzzleHttp\HandlerStack.
     * @param callable $delay Optional custom delay method to apply (default is exponential)
     * @return Client
     * @deprecated
     */
    public static function factory(array $config = [], callable $delay = null)
    {
        return new self($config, $delay);
    }

    /**
     * Create a client instance
     *
     * @param array $config Client configuration settings:
     *     - token: (optional) Storage API token.
     *     - runId: (optional) Storage API runId.
     *     - url: (optional) Syrup API URL to override the default (DEFAULT_API_URL).
     *     - super: (optional) Name of parent component if any.
     *     - userAgent: (optional) Custom user agent (appended to the default).
     *     - backoffMaxTries: (optional) Number of retries in case of backend error.
     *     - logger: (optional) instance of Psr\Log\LoggerInterface.
     *     - handler: (optional) instance of GuzzleHttp\HandlerStack.
     * @param callable $delay Optional custom delay method to apply (default is exponential)
     * @return Client
     */
    public function __construct(array $config = [], callable $delay = null)
    {
        $token = '';
        if (!empty($config['token'])) {
            $token = $config['token'];
        }
        $apiUrl = self::DEFAULT_API_URL;
        if (!empty($config['url'])) {
            $apiUrl = $config['url'];
        }
        $runId = '';
        if (!empty($config['runId'])) {
            $runId = $config['runId'];
        }
        $userAgent = self::DEFAULT_USER_AGENT;
        if (!empty($config['userAgent'])) {
            $userAgent .= ' - ' . $config['userAgent'];
        }
        $maxRetries = self::DEFAULT_BACKOFF_RETRIES;
        if (!empty($config['backoffMaxTries'])) {
            $maxRetries = $config['backoffMaxTries'];
        }

        $this->setUrl($apiUrl);
        $this->guzzle = $this->initClient(
            $token,
            $runId,
            $userAgent,
            $maxRetries,
            $config,
            $delay
        );
        if (!empty($config['super'])) {
            $this->setSuper($config['super']);
        }
    }

    protected function initClient(
        $token,
        $runId,
        $userAgent,
        $maxRetries,
        array $config = [],
        callable $delay = null)
    {
        // Initialize handlers (start with those supplied in constructor)
        if (isset($config['handler']) && $config['handler'] instanceof HandlerStack) {
            $handlerStack = HandlerStack::create($config['handler']);
        } else {
            $handlerStack = HandlerStack::create();
        }
        // Set exponential backoff for cases where job detail returns error
        $handlerStack->push(Middleware::retry(
            self::createDefaultDecider($maxRetries),
            $delay
        ));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($token, $runId, $userAgent) {
                $req = $request->withHeader('User-Agent', $userAgent);
                if ($token) {
                    $req = $req->withHeader('X-StorageApi-Token', $token);
                }
                if (!$req->hasHeader('content-type')) {
                    $req = $req->withHeader('Content-type', 'application/json');
                }
                if ($runId) {
                    $req = $req->withHeader('X-KBC-RunId', $runId);
                }
                return $req;
            }
        ));

        // Set client logger
        if (isset($config['logger']) && $config['logger'] instanceof LoggerInterface) {
            $handlerStack->push(Middleware::log(
                $config['logger'],
                new MessageFormatter(
                    "{hostname} {req_header_User-Agent} - [{ts}] \"{method} {resource} {protocol}/{version}\" " .
                    "{code} {res_header_Content-Length}"
                )
            ));
        }

        // finally create the instance
        return new GuzzleClient(['base_url' => $this->url, 'handler' => $handlerStack]);
    }

    public function getGuzzle()
    {
        return $this->guzzle;
    }


    /**
     * Set parent component.
     * @param string $super Name of the parent component.
     */
    protected function setSuper($super)
    {
        $this->super = $super;
    }


    /**
     * Set request URL
     * @param string $url Base url for requests.
     */
    protected function setUrl($url)
    {
        $this->url = $url;
    }


    /**
     * Decode a JSON response.
     * @param Response $response
     * @return array Parsed response.
     * @throws ClientException In case response cannot be read properly.
     */
    private function decodeResponse(Response $response)
    {
        $data = json_decode($response->getBody()->read($response->getBody()->getSize()), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new ClientException('Unable to parse response body into JSON: ' . json_last_error());
        }

        return $data === null ? array() : $data;
    }


    /**
     * Create a new asynchronous job.
     *
     * @param string $component Component name
     * @param array $options Available options are:
     *      - config: configuration id.
     *      - configData: configuration data.
     * @return array Response data with job status.
     * @throws ClientException
     */
    public function createJob($component, $options = array())
    {
        $uri = new Uri($this->url);
        if ($this->super) {
            $uri = $uri->withPath("{$this->super}/{$component}/run");
        } else {
            $uri = $uri->withPath("{$component}/run");
        }
        $body = [];
        if (isset($options['config'])) {
            $body['config'] = $options['config'];
        }
        if (isset($options['configData'])) {
            $body['configData'] = $options['configData'];
        }

        try {
            $request = new Request('POST', $uri, [], json_encode($body));
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }


    /**
     * Get asynchronous job status (waiting, processing, etc.).
     *
     * @param int|string $job Job Id.
     * @return array Response data with job status.
     * @throws ClientException
     */
    public function getJob($job)
    {
        $uri = new Uri($this->url);
        $uri = $uri->withPath("queue/job/{$job}");
        try {
            $request = new Request('GET', $uri);
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }


    /**
     *
     * Encrypt string
     *
     * Options array can contain `path` attribute to allow further level nesting, eg.
     * .../docker/docker-config-encrypt-verify/configs/encrypt
     *     ^ super ^ component                 ^ path
     * @param string $component Name of the component.
     * @param string $string Arbitrary string.
     * @param array $options Array with options:
     *      - path: additional nesting for request (see above)
     * @return string Encrypted string.
     * @throws ClientException
     */
    public function encryptString($component, $string, array $options = [])
    {
        if ($this->super) {
            $uriParts[] = $this->super;
        }
        $uriParts[] = $component;
        if (!empty($options['path'])) {
            $uriParts[] = $options['path'];
        }
        $uriParts[] = 'encrypt';
        $uri = new Uri($this->url);
        $uri = $uri->withPath(implode('/', $uriParts));
        try {
            $request = new Request('POST', $uri, ["Content-Type" => "text/plain"], $string);
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $response->getBody()->read($response->getBody()->getSize());
    }


    /**
     * Encrypt array.
     *
     * Options array can contain `path` attribute to allow further level nesting, eg.
     * .../docker/docker-config-encrypt-verify/configs/encrypt
     *     ^ super ^ component                 ^ path
     *
     * @param string $component Name of the component.
     * @param array $array Arbitrary array with data.
     * @param array $options Array with options:
     *      - path: additional nesting for request (see above)
     * @return array Array with encrypted values.
     * @throws ClientException
     */
    public function encryptArray($component, array $array, array $options = [])
    {
        if ($this->super) {
            $uriParts[] = $this->super;
        }
        $uriParts[] = $component;
        if (!empty($options['path'])) {
            $uriParts[] = $options['path'];
        }
        $uriParts[] = 'encrypt';
        $uri = new Uri($this->url);
        $uri = $uri->withPath(implode('/', $uriParts));
        try {
            $request = new Request('POST', $uri, ['Content-type' => 'application/json',], json_encode($array));
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }


    /**
     * Run a syrup job and wait for the result.
     *
     * @param string $component Component name.
     * @param array $options Available options are:
     *      - config: configuration id.
     *      - configData: configuration data.
     * @return array Response data with job result.
     * @throws ClientException In case creation or waiting for the job failed.
     */
    public function runJob($component, array $options = [])
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
            sleep(min(pow(2, $attempt), $this->maxDelay));
        }
        return $job;
    }
}
