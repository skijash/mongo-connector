

def add_status(response, status, message):
    response.update({'status': status, 'message': message})


def add_ok_status(response, message):
    add_status(response, 'OK', message)


def add_error_status(response, message):
    add_status(response, 'ERROR', message)
