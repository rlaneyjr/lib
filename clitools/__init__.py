"""
CLI Tools - Command Line Interface building tools

Example usage::

    from clitools import CliApp

    cli = CliApp()

    @cli.command
    def hello(args):
        print("Hello, world!")

    @cli.command
    @cli.parser_arg('--name')
    def hello2(args):
        print("Hello, {0}!".format(args.name))

    if __name__ == '__main__':
        cli.run_from_command_line()
"""

import argparse
import logging
import sys


__version__ = '0.4b1'  # sync with setup.py!


logger = logging.getLogger('clitools')


class Command(object):
    def __init__(self, func, func_info):
        self.func = func
        self.func_info = func_info
        logger.debug('-- New CliApp instance')

    def __call__(self, parsed_args):
        """
        We need to map parsed arguments to function arguments
        before calling..
        """

        args = []
        kwargs = {}

        for argname in self.func_info['positional_args']:
            args.append(getattr(parsed_args, argname))

        for argname, default in self.func_info['keyword_args']:
            kwargs[argname] = getattr(parsed_args, argname, default)

        return self.func(*args, **kwargs)


class CliApp(object):
    class arg(object):
        """Class used to wrap arguments as function defaults"""
        def __init__(self, *a, **kw):
            self.args = a
            self.kwargs = kw

    def __init__(self, prog_name='cli-app'):
        self.prog_name = prog_name
        self.parser = argparse.ArgumentParser(prog=prog_name)
        self.subparsers = self.parser.add_subparsers(help='sub-commands')

    def command(self, func=None, **kwargs):
        """
        Decorator to register a command function

        :param name: Name for the command
        :param help: Help text for the function
        """
        def decorator(func):
            self._register_command(func, **kwargs)
            return func
        if func is None:
            return decorator
        return decorator(func)

    def _register_command(self, func, **kwargs):
        """
        Register a command function. We need to hack things a bit here:

        - we need to change argument defaults in the function (copying it)
        - The original function is copied, and default values changed
        - The new function is copied in the subparser object

        WARNING! variable arguments / keyword arguments are not supported
        (yet)! They are just stripped & ignored, ATM..
        """

        func_info = self._analyze_function(func)

        ## WARNING! We're not supporting things like this, right now:
        ## def func(a, ((b, c), d)=((1, 2), 3)): pass
        ## Maybe, we should fallback to requiring "flat" arguments,
        ## at least for the moment?

        ## Read keyword arguments
        name = kwargs.get('name')
        if name is None:
            name = func_info['name']
            ## Strip the command_ prefix from function name
            if name.startswith('command_'):
                name = name[len('command_'):]

        help_text = kwargs.get('help')
        if help_text is None:
            help_text = func_info['help_text']

        ## Create the new subparser
        subparser = self.subparsers.add_parser(name, help=help_text)

        ## Process required positional arguments
        for argname in func_info['positional_args']:
            logger.debug('New argument: {0}'.format(argname))
            subparser.add_argument(argname)

        ## Process optional keyword arguments
        func_new_defaults = []
        for argname, argvalue in func_info['keyword_args']:
            if isinstance(argvalue, self.arg):
                ## We already have args / kwargs for this argument
                a = (['--' + argname] + list(argvalue.args))
                kw = argvalue.kwargs
                func_new_defaults.append(kw.get('default'))

            else:
                ## We need to guess args / kwargs from default value
                a, kw = self._arg_from_free_value(argname, argvalue)
                func_new_defaults.append(argvalue)  # just use the old one

            logger.debug('New argument: {0!r} {1!r}'.format(a, kwargs))
            subparser.add_argument(*a, **kw)
        func.func_defaults = tuple(func_new_defaults)

        ## todo: replace defaults on the original function, to strip
        ##       any instance of ``self.arg``?

        new_function = Command(func=func, func_info=func_info)

        ## Positional arguments are treated as required values
        subparser.set_defaults(func=new_function)

        return subparser  # for further analysis during tests

    def _analyze_function(self, func):
        """
        Extract information from a function:

        - positional argument names
        - optional argument names / default values
        - does it accept *args?
        - does it accept **kwargs?
        """
        import inspect

        info = {}

        info['name'] = func.func_name

        # todo extract arguments docs too!
        info['help_text'] = inspect.getdoc(func)

        argspec = inspect.getargspec(func)
        is_generator = inspect.isgeneratorfunction(func)

        info['accepts_varargs'] = argspec.varargs is not None
        info['varargs_name'] = argspec.varargs

        info['accepts_kwargs'] = argspec.keywords is not None
        info['kwargs_name'] = argspec.keywords

        info['is_generator'] = is_generator

        arg_defaults = argspec.defaults or []
        akw_limit = len(argspec.args) - len(arg_defaults)
        info['positional_args'] = argspec.args[:akw_limit]

        kwargs_names = argspec.args[akw_limit:]
        assert len(kwargs_names) == len(arg_defaults)
        info['keyword_args'] = zip(kwargs_names, arg_defaults)

        return info

    def _arg_from_free_value(self, name, value):
        """
        Guess the correct argument type to be built for free-form
        arguments (default values)
        """
        logger.debug('_arg_from_free_value({0!r}, {1!r})'.format(name, value))

        arg_name = '--' + name

        def o(*a, **kw):
            return a, kw

        if value is None:
            ## None: this is just a generic argument, accepting any value
            logger.debug('None -> generic optional argument')
            return o(arg_name, default=value)

        elif (value is True) or (value is False):
            ## Boolean value: on/off flag
            logger.debug('bool -> flag')
            action = 'store_false' if value else 'store_true'
            return o(arg_name, action=action, default=value)

        elif isinstance(value, (list, tuple)):
            ## List/tuple: if has at least two items, it will
            ## be used for a 'choice' option, else for an 'append'
            ## list.

            if len(value) > 1:
                ## Choices
                logger.debug('List with length >= 2 -> choices')
                return o(arg_name, type='choice', choices=value,
                         default=value[0])

            else:
                ## Append (of type)
                type_ = None

                logger.debug('List with length < 2 -> list of items')
                if len(value) > 0:
                    ## This is [<type>]
                    type_ = (value[0]
                             if isinstance(value[0], type)
                             else type(value[0]))
                return o(arg_name, type=type_, action='append', default=[])

        else:
            ## Anything of this type will fit..
            ## todo: make sure the type is a supported one?
            if isinstance(value, type):
                type_ = value
                default = None
            else:
                type_ = type(value)
                default = value
            logger.debug('Generic object of type {0!r} (default: {1!r})'
                         .format(type_, default))
            # import ipdb; ipdb.set_trace()
            return o(arg_name, type=type_, default=default)

    def run(self, args=None):
        """Handle running from the command line"""
        parsed_args = self.parser.parse_args(args)
        function = getattr(parsed_args, 'func', None)

        if function is None:
            ## Emulate Python2 behavior..
            self.parser.print_help(sys.stderr)
            sys.exit(2)

        # function = parsed_args.func
        return parsed_args.func(parsed_args)


## Utility methods
##----------------------------------------

def split_function_doc(doc):
    """
    Performs a very simple splitting of a function documentation:
    - separate blocks starting with :name from the rest of the
      function documentation

    Note: this function expects the passed-in docstring
          to be already cleaned, usually via pydoc.getdoc().

    :yields: two-tuples (block_info, block_data).
        - block info is a tuple of strings describing the workds
          between the first two colons, or None
        - block data is the block data without any prefix
    """

    def tokenize_blocks(lines):
        ## We need to loop until we find a line starting with :
        ## or the end of the docstring.
        buf = []
        for line in lines:
            if line.startswith(':'):
                if len(buf):
                    yield buf
                    buf = []
            buf.append(line)
        if len(buf):
            yield buf

    for block in tokenize_blocks(doc.splitlines()):
        block_data = '\n'.join(block).strip()
        if block_data.startswith(':'):
            _, args, block_data = block_data.split(':', 2)
            block_info = tuple(args.split())

        else:
            block_info = None
        yield block_info, block_data.strip()


def extract_arguments_info(doc):
    """
    Extract (organized) arguments information from a docstring.

    This will extract all the :param: and :type: arguments
    from the function docstring and return them in a dictionary,
    along with function docstring.

    >>> extract_arguments_info('''
    ... My example function.
    ...
    ... :param spam: Some spam argument
    ... :type spam: str
    ... :param int eggs: Some eggs argument
    ... :param bacon: Yummy!
    ... ''') == {
    ...     'function_help': 'My example function.\\n',
    ...     'params_help': {
    ...         'spam': {'help': 'Some spam argument', 'type': 'str'},
    ...         'eggs': {'help': 'Some eggs argument', 'type': 'int'},
    ...         'bacon': {'help': 'Yummy!'}
    ...     }
    ... }
    True
    """
    from collections import defaultdict

    func_doc = []
    args_doc = defaultdict(dict)

    for block_info, block_data in split_function_doc(doc):
        if block_info is None:
            func_doc.append(block_data)
        else:
            block_type = block_info[0]

            # :param <type> <name>: <doc>
            # :param <name>: <doc>
            # :type <name>: <type>

            if block_type in ('param', 'type'):
                if block_type == 'param' and len(block_info) == 3:
                    p_type, p_name = block_info[1:3]
                    p_help = block_data
                    args_doc[p_name]['type'] = p_type
                    args_doc[p_name]['help'] = p_help

                elif block_type == 'param' and len(block_info) == 2:
                    p_name = block_info[1]
                    p_help = block_data
                    args_doc[p_name]['help'] = p_help

                elif block_type == 'type' and len(block_info) == 2:
                    p_name = block_info[1]
                    p_type = block_data
                    args_doc[p_name]['type'] = p_type

                else:
                    raise ValueError("Wrong block information")

    return {
        'function_help': '\n'.join(func_doc).strip() + '\n',
        'params_help': dict(args_doc),
    }
