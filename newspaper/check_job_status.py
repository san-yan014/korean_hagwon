# check_batch.py
import anthropic
import os


# EUNICE
os.environ['ANTHROPIC_API_KEY'] ="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"

batch_id = 'msgbatch_01Xs7qTaWR84RBH8KNAbvSvy'

# ### CANCELING BATCH 
# client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))


# print(f"cancelling batch: {batch_id}")

# # check current status
# batch = client.messages.batches.retrieve(batch_id)
# print(f"\ncurrent status: {batch.processing_status}")
# print(f"counts: {batch.request_counts}")

# # cancel
# try:
#     cancelled = client.messages.batches.cancel(batch_id)
#     print(f"\n✓ cancel request sent")
#     print(f"new status: {cancelled.processing_status}")
    
#     if cancelled.processing_status == "canceling":
#         print("\nbatch is being cancelled...")
#         print("wait 1-2 minutes, then verify:")
#         print(f"  python -c \"import anthropic, os; client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY')); batch = client.messages.batches.retrieve('{batch_id}'); print(f'Status: {{batch.processing_status}}')\"")
    
# except Exception as e:
#     print(f"\n✗ error cancelling: {e}")



#### CHECKING THE STATUS OF THE BATCH

# check_batch.py
import anthropic
import os

client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))
batch = client.messages.batches.retrieve(batch_id)

counts = batch.request_counts
total = counts.processing + counts.succeeded + counts.errored + counts.canceled + counts.expired


print(f"status: {batch.processing_status}")
print(f"succeeded: {batch.request_counts.succeeded}/{total}")
print(f"errored: {batch.request_counts.errored}")

if batch.processing_status == "ended":
    print("\n✓ ready to download")
elif batch.processing_status in ["canceled", "canceling"]:
    print("\n❌ batch cancelled")
else:
    print("\n⏳ still processing")