from task_one import task_one
from task_two import task_two
from task_three import task_three
from task_four import task_four
from task_five import task_five
from task_six import task_six
from task_seven import task_seven
from task_eight import task_eight
from Read_Write import write_df
from patch_to_save import *


def main():
    one_result = task_one()
    two_result = task_two()
    three_result = task_three()
    four_result = task_four()
    fife_result = task_five()
    six_result = task_six()
    seven_result = task_seven()
    eight_result = task_eight()
    write_df(one_result, one_task_patch)
    write_df(two_result, two_task_patch)
    write_df(three_result, three_task_patch)
    write_df(four_result, four_task_patch)
    write_df(fife_result, five_task_patch)
    write_df(six_result, six_task_patch)
    write_df(seven_result, seven_task_patch)
    write_df(eight_result, eight_task_patch)


def print_hi(name):
    print(f'Hi, {name}')


if __name__ == '__main__':
    main()
    print_hi('This is test function')
