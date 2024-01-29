ERROR_HELP_STRINGS = {
    # Common Errors
    'InternalServerError': 'Internal Server Error, generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException': 'Request rate is too high. If you\'re using a custom retry strategy make sure to retry with exponential back-off.' +
                                              'Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index',
    'ResourceNotFoundException': 'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable': 'Had trouble reaching DynamoDB. generally safe to retry with exponential back-off',
    'ThrottlingException': 'Request denied due to throttling, generally safe to retry with exponential back-off',
    'UnrecognizedClientException': 'The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException': 'The input fails to satisfy the constraints specified by DynamoDB, fix input before retrying',
    'RequestLimitExceeded': 'Throughput exceeds the current throughput limit for your account, increase account level throughput before retrying',
    'ConditionalCheckFailedException': 'Conditional Check Failed for current operation',
     'ResourceInUseException': 'Table already exists.'
}

def handle_error(error):
    if type(error) == Exception:
        print('An exception has occured. Error message: {error_message}'
              .format(error_message=str(error)))
    else:
        error_code = error.response['Error']['Code'] if not None else error.response['Error']['Code']

        error_message = error.response['Error']['Message']

        error_help_string = ERROR_HELP_STRINGS[error_code]

        print('[{error_code}] {help_string}. Error message: {error_message}'
              .format(error_code=error.response,
                      help_string=error_help_string,
                      error_message=error.response))


