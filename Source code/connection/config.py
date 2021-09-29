from configparser import ConfigParser


def config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    configuration = {}

    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            configuration[item[0]] = item[1]
    else:
        raise Exception('Configuration file was not found.')

    return configuration